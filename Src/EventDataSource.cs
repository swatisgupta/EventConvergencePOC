using Azure;
using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.Schemas;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using PartitionKey = Microsoft.Azure.Cosmos.PartitionKey;

namespace EventConvergencePOCTest.Src
{
    internal class EventDataSource
    {
        CosmosClient cosmosClient;
        Dictionary<string, Container> EventContainers;
        List<string> EventContainerIds = new List<string> { "Event" };
        Database EventDatabase;
        string databaseId = "EventsTest1";
        List<string> EventSummaryContainerId = new List<string> { "EventSummary" };

        public static string BindingStateActive = "Active";
        public static string BindingStateInActive = "InActive";
        public static string Satisfied = "Satisfy";
        public static string Canceled = "Cancel";
        public static string Registered = "Register";


        double requestCharge = 0;
        double requestExecutionTime = 0;
        List<string> requestChargePerDbOperation = new List<string>();

        public void ClearCharge()
        {
            requestChargePerDbOperation?.Clear();
        }

        public List<string> GetRUPerDBoperation()
        {
            return requestChargePerDbOperation;
        }

        public EventDataSource(CosmosClient cosmosClient)
        {
            this.cosmosClient = cosmosClient;
        }

        private void ResetTimmings()
        {
            requestCharge = 0;
            requestExecutionTime = 0;
        }
        public void GetEventContainers()
        {
            this.EventContainers = new Dictionary<string, Container>();
            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;

            FeedIterator<ContainerProperties> iterator = EventDatabase.GetContainerQueryIterator<ContainerProperties>();
            FeedResponse<ContainerProperties> containers = iterator.ReadNextAsync().ConfigureAwait(false).GetAwaiter().GetResult();


            for (int i = 0; i < containers.Count; i++)
            {
                var containerId = containers.ElementAt(i).Id;

                if (EventContainerIds.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (this.EventContainerIds.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    this.EventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
                else
                {
                    this.EventContainerIds.Append(containerId);
                    this.EventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
            }

            foreach (var containerId in EventContainerIds)
            {
                if (EventContainers.ContainsKey(containerId))
                {
                    continue;
                }
                var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);

                string partitionKey = "/pk1";
                var container = EventDatabase.DefineContainer(containerId, partitionKey)
                    .WithIndexingPolicy()
                    .WithAutomaticIndexing(false)
                    .WithIncludedPaths()
                    .Path("/")
                    .Path("/PublishingEvent/?")
                    .Path("/Scope/?")
                    .Path("/JobId/?")
                    .Path("/TaskId/?")
                    .Path("/Action/?")
                    .Path("/EventTimestamp/?")
                    .Attach()
                    .WithExcludedPaths()
                    .Path("/AdditionalScopes/*")
                    .Path("/JobType/?")
                    .Path("/Workflow/?")
                    .Path("/AssociatedPrerequisites/*")
                    .Path("/Arguments/*")
                    .Path("/BindingState/?")
                    .Path("/TimeToSatisfy/?")
                    .Path("/_ts/?")
                    .Path("/_etag/?")
                    .Attach()
                    .Attach()
                    .CreateIfNotExistsAsync(throughputProperties).GetAwaiter().GetResult().Container;
                EventContainers[containerId] = container;
            }
        }


        public Tuple<double, double> UpdateEvent(Event newState, string etag = null)
        {
            try
            {
                newState.pk1 = $"{newState.PublishingEvent}-{newState.Scope}".ToLower();
                newState.id = $"{newState.JobId}-{newState.TaskId}".ToLower();
                var response = EventContainers[EventContainerIds[0]].UpsertItemAsync<Event>(newState, new PartitionKey(newState.pk1)).GetAwaiter().GetResult();
                requestChargePerDbOperation.Add($"Write {response.RequestCharge}");

                return new Tuple<double, double>(response.RequestCharge, response.Diagnostics.GetClientElapsedTime().TotalMilliseconds);
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        public List<Event> GetAllJobsEmitOrRegisterEvent(List<string> EventNames, string scope)
        {
            var EventContainer = EventContainers[EventContainerIds[0]];
            List<Event> resultsList = new List<Event>();
            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.BindingState = '{BindingStateActive}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = EventContainer.GetItemQueryIterator<Event>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
                    requestChargePerDbOperation.Add($"Read {results.RequestCharge}");

                    this.requestExecutionTime += results.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    if (results != null)
                    {
                        resultsList.AddRange(results.Where((x) => x.Action != Canceled)?.ToList());
                    }

                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return resultsList;
        }

        public List<Event> GetAllJobsEmitEvent(List<string> EventNames, string scope)
        {
            var EventContainer = EventContainers[EventContainerIds[0]];
            List<Event> resultsList = new List<Event>();
            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.Status = '{Satisfied}' AND c.BindingState = '{BindingStateActive}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = EventContainer.GetItemQueryIterator<Event>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
                    requestChargePerDbOperation.Add($"Read {results.RequestCharge}");

                    this.requestExecutionTime += results.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    if (results != null)
                    {
                        resultsList.AddRange(results.ToList());
                    }

                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return resultsList;
        }

        public List<Event> GetAllJobsRegisterEvent(List<string> EventNames, string scope)
        {
            var EventContainer = EventContainers[EventContainerIds[0]];
            List<Event> resultsList = new List<Event>();

            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.pk1 = '{name.ToLower()}-{scope.ToLower()}' AND c.Status = '{Registered}' AND c.BindingState = '{BindingStateActive}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = EventContainer.GetItemQueryIterator<Event>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
                    requestChargePerDbOperation.Add($"Read {results.RequestCharge}");

                    this.requestExecutionTime += results.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    if (results != null)
                    {
                        resultsList.AddRange(results.ToList());
                    }

                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return resultsList;
        }

        public Event GetEventForJob(string jobId, string taskId, List<string> altNames, string scope)
        {
            var EventContainer = EventContainers[EventContainerIds[0]];
            ResetTimmings();

            try
            {
                if (altNames.Count > 0)
                {
                    var query = $"SELECT * FROM c WHERE c.JobId = '{jobId}' AND c.TaskId = '{taskId}' AND c.Scope='{scope}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = EventContainer.GetItemQueryIterator<Event>(queryDefinition);
                    var response = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    requestChargePerDbOperation.Add($"Read {response.RequestCharge}");

                    this.requestCharge += response.RequestCharge;
                    this.requestExecutionTime += response.Diagnostics.GetClientElapsedTime().TotalMilliseconds;

                    if (response != null)
                    {
                        foreach (var entry in response)
                        {
                            if (altNames.Contains(entry.PublishingEvent) && scope.Equals(entry.Scope))
                            {
                                return entry;
                            }

                        }
                    }
                }
                else
                {
                    var id = $"{jobId}-{taskId}";
                    var partitionKey = new PartitionKey($"{altNames[0].ToLower()}-.{scope.ToLower()}");
                    var response = EventContainer.ReadItemAsync<Event>(id, partitionKey).GetAwaiter().GetResult();
                    this.requestCharge += response.RequestCharge;
                    requestChargePerDbOperation.Add($"Read {response.RequestCharge}");

                    this.requestExecutionTime += response.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    return response.Resource;
                }

            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }

            return null;
        }

        public List<Event> GetEventForQuery(string query)
        {
            var EventContainer = EventContainers[EventContainerIds[0]];
            ResetTimmings();

            try
            {
                var queryIterator = EventContainer.GetItemQueryIterator<Event>(query);
                var response = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                requestChargePerDbOperation.Add($"Read {response.RequestCharge}");

                this.requestCharge += response.RequestCharge;
                this.requestExecutionTime += response.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                response.Resource.ToList();

            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }

            return null;
        }

        public double GetRequestCharge()
        {
            return requestCharge;
        }

        public double GetRequestExecutionTime()
        {
            return requestExecutionTime;
        }
    }
}
