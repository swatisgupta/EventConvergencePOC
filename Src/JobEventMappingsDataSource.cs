using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Src
{
    internal class JobEventMappingsDataSource
    {
        CosmosClient cosmosClient;
        Dictionary<string, Container> JobEventContainers;
        List<string> JobEventsContainerId = new List<string> { "JobEventsMapping" };
        Database EventDatabase;
        string databaseId = "Events";
        List<string> EventsContainerId = new List<string> { "Events" };

        public static string BindingStateActive = "Active";
        public static string BindingStateInActive = "InActive";
        public static string Satisfied = "Satisfied";
        public static string Canceled = "Canceled";
        public static string Registered = "Registered";


        double requestCharge = 0;
        double requestExecutionTime = 0;

        public JobEventMappingsDataSource (CosmosClient cosmosClient)
        {
            this.cosmosClient = cosmosClient;
        }

        private void ResetTimmings()
        {
            requestCharge = 0;
            requestExecutionTime = 0;
        }
        public void GetJobEventContainers()
        {
            this.JobEventContainers = new Dictionary<string, Container>();
            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;

            FeedIterator<ContainerProperties> iterator = EventDatabase.GetContainerQueryIterator<ContainerProperties>();
            FeedResponse<ContainerProperties> containers = iterator.ReadNextAsync().ConfigureAwait(false).GetAwaiter().GetResult();


            for(int i=0; i< containers.Count; i++)
            {
                var containerId = containers.ElementAt(i).Id;

                if ( EventsContainerId.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    continue;
                }

                if(this.JobEventsContainerId.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    this.JobEventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
                else
                {
                    this.JobEventsContainerId.Append(containerId);
                    this.JobEventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
            }

            foreach (var containerId in JobEventsContainerId)
            {
                if (JobEventContainers.ContainsKey(containerId))
                {
                    continue;
                }

                List<string> subPartitionKeyPaths = new List<string> {
                                                    "/pk1",
                                                    "/pk2" };
                var containerProperties = new ContainerProperties(containerId, partitionKeyPaths: subPartitionKeyPaths);
                var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);
                JobEventContainers[containerId] = EventDatabase.CreateContainerIfNotExistsAsync(
                    containerProperties,
                    throughputProperties).GetAwaiter().GetResult()?.Container;

            }
        }

        public Tuple<double, double> UpdateJobEventMappings(JobEventMappings newState)
        {
            try
            {
                newState.pk1 = $"{newState.PublishingEvent}".ToLower();
                newState.pk2 = $"{newState.Scope}".ToLower();
                newState.id = $"{newState.JobId}-{newState.TaskId}".ToLower();
                var response = JobEventContainers[JobEventsContainerId[0]].UpsertItemAsync<JobEventMappings>(newState).GetAwaiter().GetResult();
                return new Tuple<double, double>(response.RequestCharge, response.Diagnostics.GetClientElapsedTime().TotalMilliseconds);
            }
            catch(Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        public List<JobEventMappings> GetAllJobsEmitOrRegisterEvent(List<string> EventNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            List<JobEventMappings> resultsList = new List<JobEventMappings>();
            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.BindingState = '{BindingStateActive}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
                    this.requestExecutionTime += results.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    if (results != null)
                    {
                        resultsList.AddRange(results.Where((x) => x.Status != Canceled)?.ToList());
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

        public List<JobEventMappings> GetAllJobsEmitEvent(List<string> EventNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            List<JobEventMappings> resultsList = new List<JobEventMappings>();
            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.Status = 'Satisfied' AND c.BindingState = 'Active'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
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

        public List<JobEventMappings> GetAllJobsRegisterEvent(List<string> EventNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            List<JobEventMappings> resultsList = new List<JobEventMappings>();
            
            ResetTimmings();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.pk1 = '{name.ToLower()}' AND c.pk2 = '{scope.ToLower()}' AND c.Status = 'Registered' AND c.BindingState = 'Active'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.requestCharge += results.RequestCharge;
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

        public JobEventMappings GetEventForJob(string jobId, string taskId, List<string> altNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            ResetTimmings();

            try
            {
                if (altNames.Count > 0)
                {
                    var query = $"SELECT * FROM c WHERE c.JobId = '{jobId}' AND c.TaskId = '{taskId}' AND c.pk2='{scope.ToLower()}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var response = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
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
                    var partitionKey = new PartitionKeyBuilder().Add(altNames[0].ToLower()).Add(scope.ToLower()).Build();
                    var response = JobEventMappingsContainer.ReadItemAsync<JobEventMappings>(id, partitionKey).GetAwaiter().GetResult();
                    this.requestCharge += response.RequestCharge;
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
