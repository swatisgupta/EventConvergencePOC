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
    internal class EventsDataSource
    {
        CosmosClient cosmosClient;
        Database EventDatabase;
        Dictionary<string, Container> EventsContainers;
        static string databaseId = "EventsTest1";
        List<string> EventsContainerId = new List<string> { "Events1" };

        public static string Canceled = "Canceled";
        public static string Registered = "Registered";
        public static string Satisfied = "Satisfied";
        public static string Obsolete = "Obsolete";


        double requestCharge = 0;
        double requestExecutionTime = 0;
        List<string> requestChargePerDbOperation = new List<string>();

        CatalogDataSource catalogDataSource;
        JobEventMappingsDataSource jobEventMappingsDataSource;

        public EventsDataSource(CosmosClient cosmosClient)
        {
            requestChargePerDbOperation.Clear();
            this.cosmosClient = cosmosClient;
            catalogDataSource = new CatalogDataSource(cosmosClient);
            jobEventMappingsDataSource = new JobEventMappingsDataSource(cosmosClient);
        }

        public EventsDataSource(CosmosClient cosmosClient, CatalogDataSource catalogDataSource, JobEventMappingsDataSource jobEventMappingsDS)
        {
            this.cosmosClient = cosmosClient;
            this.catalogDataSource = catalogDataSource;
            this.jobEventMappingsDataSource = jobEventMappingsDS;
        }

        public void ClearCharge()
        {
            requestChargePerDbOperation?.Clear();
        }

        public List<string> GetRUPerDBoperation()
        {
            return requestChargePerDbOperation;
        }


        public void GetEventsContainer()
        {
            if (EventsContainers != null)
            {
                return;
            }


            EventsContainers = new Dictionary<string, Container>();

            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;
            var partitionKey = "/pk";
            var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);

            var container = EventDatabase.DefineContainer(this.EventsContainerId[0], partitionKey)
                .WithIndexingPolicy()
                .WithAutomaticIndexing(false)
                .WithIncludedPaths()
                .Path("/")
                .Path("/EventName/?")
                .Path("/Scope/?")
                .Path("/pk/?")
                .Path("/AdditionalScopes/[]/Scope/?")
                .Path("/AssociatedJobId/?")
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
                .Path("/TimeToSatisfy/?")
                .Path("/ExpirationTime/?")
                .Path("/Arguments/*")
                .Path("/SatisfyBy/?")
                .Path("/Status/?")
                .Attach()
                .Attach()
                .CreateIfNotExistsAsync(throughputProperties).GetAwaiter().GetResult().Container;

            this.EventsContainers[EventsContainerId[0]] = container;
        }

        private void resetTimings()
        {
            requestCharge = 0;
            requestExecutionTime = 0;
        }

        public CosmosDbItem<Events> GetEvent(List<string> altNames, string scope)
        {
            resetTimings();

            // Find event with all possible event names
            foreach (var name in altNames)
            {
                try
                {
                    CosmosDbItem<Events> cosmosDBItem = new CosmosDbItem<Events>();
                    var id = $"{scope}".ToLower();
                    var pk = name.ToLower();
                    var eventsEntry = EventsContainers[EventsContainerId[0]].ReadItemAsync<Events>(id, new PartitionKey(pk)).GetAwaiter().GetResult();
                    requestCharge += eventsEntry.RequestCharge;
                    requestExecutionTime += eventsEntry.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    requestChargePerDbOperation.Add($"Read {eventsEntry.RequestCharge}");

                    if (eventsEntry != null && eventsEntry.Resource.Status != Obsolete)
                    {
                        cosmosDBItem.payload = eventsEntry.Resource;
                        cosmosDBItem.etag = eventsEntry.ETag;
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

        public bool RegisterEvent(Events oldState, out Events newState, string EventName, string scope, bool HardSet, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string, string>> prereq = null, string jobType = null, string workflow = null)
        {
            bool isStateChanged = true;

            newState = new Events
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
                SatisfyBy = 0,
                Status = Registered
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

        public bool SatisfyEvent(Events oldState, out Events newState, string EventName, string scope, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string, string>> prereq = null, string jobType = null, string workflow = null)
        {
            bool isStateChanged = true;

            newState = new Events
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                AssociatedJobId = jobId,
                AssociatedTaskId = taskId,
                Arguments = EventArguments,
                AssociatedJobType = jobType,
                AssociatedWorkflow = workflow,
                Status = Satisfied,
                AssociatedPrerequisites = prereq,
                SatisfyBy = jobId == null ? 0 : 1,
                AdditionalScopes = AdditionalScopes
            };

            if (oldState != null)
            {
                if (oldState.Status == Satisfied)
                {
                    if (jobId == null || (oldState.AssociatedJobId == jobId && oldState.AssociatedTaskId == taskId))
                    {
                        isStateChanged = false;
                    }
                    else
                    {
                        newState.SatisfyBy += oldState.SatisfyBy;
                    }
                }
            }

            return isStateChanged;
        }

        public bool CancelEvent(Events oldState, out Events newState, string EventName, List<string> altNames, string scope, bool HardSet = false, string jobId = null, string taskId = null)
        {
            bool isStateChanged = true;

            newState = new Events
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                Status = Canceled,
                // SatisfyBy = 0,
            };

            if (oldState == null)
            {
                throw new Exception("Operation not allowed");
            }

            List<JobEventMappings> jobEventMappings = null;

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

            if (oldState.Status == Registered)
            {
                throw new Exception("Cancel was called on a Registered Event. If the event is intended to be canceled, then call with HardSet = true !");
            }

            if (oldState.Status == Satisfied)
            {
                newState.SatisfyBy = oldState.SatisfyBy--;

                if (newState.SatisfyBy == 0 || (oldState.AssociatedTaskId == taskId && oldState.AssociatedJobId == jobId))
                {
                    jobEventMappings = this.jobEventMappingsDataSource.GetAllJobsEmitOrRegisterEvent(altNames, scope);
                    // Find the last job that published or registered this event based on the event status
                    var mappings = jobEventMappings?.Where((x) => (x.JobId == jobId && x.TaskId == taskId));
                    if (mappings != null && mappings.Count() > 0)
                    {
                        jobEventMappings?.Remove(mappings?.First());
                    }

                    var jobMappings1 = jobEventMappings.Where((x) => x.Status == Satisfied)?.ToList();
                    var jobMappings2 = jobEventMappings.Where((x) => x.Status == Registered)?.ToList();
                    jobEventMappings = jobMappings1?.Count() > 0 ? jobMappings1 : jobMappings2;

                    JobEventMappings JobData = null;
                    jobEventMappings?.Sort((x, y) => DateTime.Compare(x.EventTimestamp, y.EventTimestamp));
                    if (jobEventMappings != null && jobEventMappings.Count > 0)
                    {
                        JobData = jobEventMappings.LastOrDefault();
                        newState.Status = JobData.Status;
                        newState.AssociatedJobId = JobData.JobId;
                        newState.AssociatedTaskId = JobData.TaskId;
                        newState.AssociatedJobType = JobData.JobType;
                        newState.AssociatedWorkflow = JobData.Workflow;
                        newState.Arguments = JobData.Arguments;
                        newState.AssociatedPrerequisites = JobData.AssociatedPrerequisites;
                        newState.AdditionalScopes = JobData.AdditionalScopes;
                    }
                    else
                    {
                        newState.Status = Canceled;
                        newState.SatisfyBy = 0;
                    }
                }

                if (newState.SatisfyBy > 1)
                {
                    newState.Status = Satisfied;
                    newState.AssociatedJobId = oldState.AssociatedJobId;
                    newState.AssociatedTaskId = oldState.AssociatedTaskId;
                    newState.AssociatedJobType = oldState.AssociatedJobType;
                    newState.AssociatedWorkflow = oldState.AssociatedWorkflow;
                    newState.Arguments = oldState.Arguments;
                    newState.AssociatedPrerequisites = oldState.AssociatedPrerequisites;
                    newState.AdditionalScopes = oldState.AdditionalScopes;
                }
            }

            return isStateChanged;
        }

        public void ComputeState(string EventName, List<string> altNames, string Scope, out CosmosDbItem<Events> newRecord, DateTime eventTimestamp, bool )
        {

            newRecord = new CosmosDbItem<Events>();
            var newState = new Events() { EventName = EventName, Scope = Scope, EventTimestamp = eventTimestamp };

            var allJobEventMappings = this.jobEventMappingsDataSource.GetAllJobsEmitOrRegisterEvent(altNames, Scope);
            // Find the last job that published or registered this event based on the event status
            var jobEventMappings = useTimestamp ? allJobEventMappings.Where((x) => x.Status == Satisfied && x.EventTimestamp > newState.EventTimestamp)?.ToList() :
                                              allJobEventMappings.Where((x) => x.Status == Satisfied)?.ToList();

            if (jobEventMappings?.Count() > 0)
            {
                jobEventMappings = useTimestamp ? allJobEventMappings.Where((x) => x.Status == Registered && x.EventTimestamp > newState.EventTimestamp)?.ToList() :
                                              allJobEventMappings.Where((x) => x.Status == Registered)?.ToList();
            }

            JobEventMappings JobData = null;

            jobEventMappings?.Sort((x, y) => DateTime.Compare(x.EventTimestamp, y.EventTimestamp));

            if (jobEventMappings != null && jobEventMappings.Count > 0)
            {
                JobData = jobEventMappings.LastOrDefault();
                newState.Status = JobData.Status;
                newState.AssociatedJobId = JobData.JobId;
                newState.AssociatedTaskId = JobData.TaskId;
                newState.AssociatedJobType = JobData.JobType;
                newState.AssociatedWorkflow = JobData.Workflow;
                newState.Arguments = JobData.Arguments;
                newState.AssociatedPrerequisites = JobData.AssociatedPrerequisites;
                newState.AdditionalScopes = JobData.AdditionalScopes;
                newState.SatisfyBy = 0;
                newState.TimeToSatisfy = JobData.TimeToSatisfy;
            }
            else
            {
                newState.Status = Canceled;
                newState.SatisfyBy = 0;
            }

            newState.EventTimestamp = DateTime.Now;
            newRecord.payload = newState;
        }

        public bool ComputeEventState(string EventName, List<string> altNames, string Scope, out CosmosDbItem<Events> newRecord, bool useTimestamp = true)
        {

            var eventResponse = GetEvent(altNames, Scope);

            var EventTimestamp = DateTime.MinValue;

            if (useTimestamp && eventResponse != null)
            {
                EventTimestamp = eventResponse.payload.EventTimestamp;
            }

            this.ComputeState(EventName, altNames, Scope, out newRecord, EventTimestamp);

            newRecord.etag = eventResponse?.etag;
            var newState = newRecord.payload;

            if(eventResponse != null && newState.Status == eventResponse.payload.Status &&
                newState.AssociatedJobId == eventResponse.payload.AssociatedJobId &&
                newState.AssociatedTaskId == eventResponse.payload.AssociatedTaskId)
            {
                return false;
            }
            return true;
        }

        public double GetRequestCharge()
        {
            return requestCharge;
        }

        public double GetExecutionTime()
        {
            return requestExecutionTime;
        }

        public Tuple<double, double> UpdateEvent(CosmosDbItem<Events> record)
        {
            /* Update the new state */
            try
            {
                record.payload.id = record.payload.Scope.ToLower();
                record.payload.pk = record.payload.EventName.ToLower();
                record.payload.EventTimestamp = DateTime.Now;

                ItemResponse<Events> response = null;
                if (record.etag != null)
                {
                    response = EventsContainers[EventsContainerId[0]].ReplaceItemAsync<Events>(record.payload, record.payload.id, new PartitionKey(record.payload.pk), requestOptions: new ItemRequestOptions() { IfMatchEtag = record.etag }).GetAwaiter().GetResult();
                }
                else
                {
                    response = EventsContainers[EventsContainerId[0]].CreateItemAsync<Events>(record.payload, new PartitionKey(record.payload.pk)).GetAwaiter().GetResult();
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
