using Azure;
using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Container = Microsoft.Azure.Cosmos.Container;

namespace EventConvergencePOCTest.Src
{
    internal class EventSummaryDataSource
    {
        CosmosClient cosmosClient;
        Database EventDatabase;
        Dictionary<string, Container> EventSummaryContainers;
        static string databaseId = "EventSummaryTest1";
        List<string> EventSummaryContainerIds = new List<string> { "EventSummary1" };

        public static string Canceled = "Canceled";
        public static string Registered = "Registered";
        public static string Satisfied = "Satisfied";
        public static string Obsolete = "Obsolete";


        double requestCharge = 0;
        double requestExecutionTime = 0;
        List<string> requestChargePerDbOperation = new List<string>();

        CatalogDataSource catalogDataSource;
        EventDataSource EventDataSource;

        public EventSummaryDataSource(CosmosClient cosmosClient)
        {
            requestChargePerDbOperation.Clear();
            this.cosmosClient = cosmosClient;
            catalogDataSource = new CatalogDataSource(cosmosClient);
            EventDataSource = new EventDataSource(cosmosClient);
        }

        public EventSummaryDataSource(CosmosClient cosmosClient, CatalogDataSource catalogDataSource, EventDataSource EventDS)
        {
            this.cosmosClient = cosmosClient;
            this.catalogDataSource = catalogDataSource;
            this.EventDataSource = EventDS;
        }

        public void ClearCharge()
        {
            requestChargePerDbOperation?.Clear();
        }

        public List<string> GetRUPerDBoperation()
        {
            return requestChargePerDbOperation;
        }


        public void GetEventSummaryContainer()
        {
            if (EventSummaryContainers != null)
            {
                return;
            }


            EventSummaryContainers = new Dictionary<string, Container>();

            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;
            var partitionKey = "/pk";
            var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);

            var container = EventDatabase.DefineContainer(this.EventSummaryContainerIds[0], partitionKey)
                .WithIndexingPolicy()
                .WithAutomaticIndexing(false)
                .WithIncludedPaths()
                .Path("/")
                .Path("/EventName/?")
                .Path("/Scope/?")
                .Path("/AdditionalScopes/[]/Scope/?")
                .Path("/EventTimestamp/*")
                .Attach()
                .WithExcludedPaths()
                .Path("/_ts/?")
                .Path("/_etag/?")
                .Path("/AdditionalScopes/[]/Name/?")
                .Path("/AssociatedTaskId/?")
                .Path("/AssociatedJobType/?")
                .Path("/AssociatedWorkflow/?")
                .Path("/AssociatedPrerequisites/*")
                .Path("/FirstSatisfactionTimestamp/?")
                .Path("/AssociatedJobId/?")
                .Path("/TimeToSatisfy/?")
                .Path("/ExpirationTime/?")
                .Path("/Arguments/*")
                .Path("/Status/?")
                .Attach()
                .Attach()
                .CreateIfNotExistsAsync(throughputProperties).GetAwaiter().GetResult().Container;

            this.EventSummaryContainers[EventSummaryContainerIds[0]] = container;
        }

        private void resetTimings()
        {
            requestCharge = 0;
            requestExecutionTime = 0;
        }

        public CosmosDbItem<EventSummary> GetEvent(List<string> altNames, string scope)
        {
            resetTimings();

            // Find event with all possible event names
            foreach (var name in altNames)
            {
                try
                {
                    CosmosDbItem<EventSummary> cosmosDBItem = new CosmosDbItem<EventSummary>();
                    var id = $"{scope}".ToLower();
                    var pk = name.ToLower();
                    var EventSummaryEntry = EventSummaryContainers[EventSummaryContainerIds[0]].ReadItemAsync<EventSummary>(id, new PartitionKey(pk)).GetAwaiter().GetResult();
                    requestCharge += EventSummaryEntry.RequestCharge;
                    requestExecutionTime += EventSummaryEntry.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    requestChargePerDbOperation.Add($"Read {EventSummaryEntry.RequestCharge}");

                    if (EventSummaryEntry != null && EventSummaryEntry.Resource.Status != Obsolete)
                    {
                        cosmosDBItem.payload = EventSummaryEntry.Resource;
                        cosmosDBItem.etag = EventSummaryEntry.ETag;
                        return cosmosDBItem;
                    }

                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Ignore
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    throw;
                }
            }

            return null;
        }


        public List<EventSummary> GetEventByQuery(string query)
        {
            resetTimings();
            List<EventSummary> items = new List<EventSummary>();
            // Find event with all possible event names
                try
                {
                    CosmosDbItem<EventSummary> cosmosDBItem = new CosmosDbItem<EventSummary>();
                    var eventSummaryEntry = EventSummaryContainers[EventSummaryContainerIds[0]].GetItemQueryIterator<EventSummary>(query);

                    while(eventSummaryEntry.HasMoreResults)
                    {
                       var response = eventSummaryEntry.ReadNextAsync().GetAwaiter().GetResult();
                       requestCharge += response.RequestCharge;
                       requestExecutionTime += response.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                       requestChargePerDbOperation.Add($"Read {response.RequestCharge}");
                       items.AddRange(response.ToList());
                    }

                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Ignore
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    throw;
                }
            return null;
        }

        public bool RegisterEvent(EventSummary oldState, out EventSummary newState, string EventName, string scope, bool HardSet, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<string> AdditionalScopes = null, List<Dictionary<string, string>> prereq = null, string jobType = null, string workflow = null)
        {
            bool isStateChanged = true;

            newState = new EventSummary
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                AssociatedJobId = jobId,
                AssociatedTaskId = taskId,
                Arguments = EventArguments,
                AssociatedJobType = jobType,
                AssociatedWorkflow = workflow,
                AdditionalScopes = AdditionalScopes,
                AssociatedPrerequisites = prereq,
                Status = Registered,
            };

            if (!HardSet && oldState != null)
            {
                if (oldState.Status == Satisfied)
                {
                    isStateChanged = false;
                }
                // merge arguments??
            }

            return isStateChanged;
        }

        public bool SatisfyEvent(EventSummary oldState, out EventSummary newState, string EventName, string scope, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<string> AdditionalScopes = null, List<Dictionary<string, string>> prereq = null, string jobType = null, string workflow = null)
        {
            bool isStateChanged = true;

            newState = new EventSummary
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                FirstSatisfactionTimestamp = DateTime.UtcNow,
                AssociatedJobId = jobId,
                AssociatedTaskId = taskId,
                Arguments = EventArguments,
                AssociatedJobType = jobType,
                AssociatedWorkflow = workflow,
                Status = Satisfied,
                AssociatedPrerequisites = prereq,
                AdditionalScopes = AdditionalScopes
            };

            if (oldState != null)
            {
                if (oldState.Status == Satisfied)
                {
                    if (jobId == null || (oldState.AssociatedJobId != null && oldState.AssociatedTaskId !=null ))
                    {
                        isStateChanged = false;
                    }
                    else
                    {
                        newState.FirstSatisfactionTimestamp = oldState.FirstSatisfactionTimestamp;
                    }
                }
            }

            return isStateChanged;
        }

        public bool CancelEvent(EventSummary oldState, out EventSummary newState, string EventName, List<string> altNames, string scope, bool HardSet = false, string jobId = null, string taskId = null)
        {
            bool isStateChanged = true;

            newState = new EventSummary
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                Status = Canceled,
            };

            if (oldState == null)
            {
                throw new Exception("Operation not allowed");
            }

            List<Event> Event = null;

            if (oldState.Status == Canceled)
            {
                // Event already in canceled state - no need to update
                isStateChanged = false;
                return isStateChanged;
            }

            if (HardSet)
            {
                // Stateless operation requested through the Rest API call
                return isStateChanged;
            }


            var allEvent = this.EventDataSource.GetAllJobsEmitOrRegisterEvent(altNames, scope);
            // Find the last job that published or registered this event based on the event status

            var events = allEvent?.Where((x) => x.BindingState == EventDataSource.BindingStateActive)?.ToList();

            if (events?.Count() > 0)
            {
                isStateChanged = false;

                if(jobId != null && oldState.AssociatedJobId == jobId && oldState.AssociatedTaskId == taskId)
                {
                    isStateChanged = true;
                    newState.Status = oldState.Status;
                    var eventsWithStatus = events.Where((x) => x.Action.Equals(oldState.Status)).ToList();
                    eventsWithStatus.Sort((x, y) => x.EventTimestamp.CompareTo(y.EventTimestamp));

                    var newMetadata = eventsWithStatus.FirstOrDefault();

                    newState.AssociatedJobId = newMetadata.JobId;
                    newState.AssociatedTaskId = newMetadata.TaskId;
                    newState.FirstSatisfactionTimestamp = oldState.FirstSatisfactionTimestamp;
                    newState.AssociatedWorkflow = newMetadata.Workflow;
                    newState.AssociatedPrerequisites = newMetadata.AssociatedPrerequisites;
                    newState.AdditionalScopes = newMetadata.AdditionalScopes;
                    newState.Arguments = newMetadata.Arguments; 
                }
            }

            return isStateChanged;
        }


        public double GetRequestCharge()
        {
            return requestCharge;
        }

        public double GetExecutionTime()
        {
            return requestExecutionTime;
        }

        public Tuple<double, double> UpdateEvent(EventSummary eventItem, string etag = null)
        {
            /* Update the new state */
            try
            {

                ItemResponse<EventSummary> response = null;
                if (etag != null)
                {
                    response = EventSummaryContainers[EventSummaryContainerIds[0]].ReplaceItemAsync<EventSummary>(eventItem, eventItem.id, new PartitionKey(eventItem.pk), requestOptions: new ItemRequestOptions() { IfMatchEtag = etag }).GetAwaiter().GetResult();
                }
                else
                {
                    response = EventSummaryContainers[EventSummaryContainerIds[0]].CreateItemAsync<EventSummary>(eventItem, new PartitionKey(eventItem.pk)).GetAwaiter().GetResult();
                }
                requestChargePerDbOperation.Add($"Write {response.RequestCharge}");
                return new Tuple<double, double>(response.RequestCharge, response.Diagnostics.GetClientElapsedTime().TotalMilliseconds);
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
        }
    }
}
