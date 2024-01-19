using Azure;
using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Src
{
    internal class EventsDataSource
    {
        CosmosClient cosmosClient;
        Database EventDatabase;
        Dictionary<string, Container> EventsContainers;
        static string databaseId = "Events";
        List<string> EventsContainerId = new List<string> { "Events" };

        public static string Canceled = "Canceled";
        public static string Registered = "Registered";
        public static string Satisfied = "Satisfied";
        public static string Obsolete = "Obsolete";


        double requestCharge = 0;
        double requestExecutionTime = 0;

        CatalogDataSource catalogDataSource;
        JobEventMappingsDataSource jobEventMappingsDataSource;

        public EventsDataSource(CosmosClient cosmosClient)
        {
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

        public void GetEventsContainer()
        {
            if (EventsContainers != null)
            {
                return;
            }

            EventsContainers = new Dictionary<string, Container>();

            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;
            var partitionKey = "pk";
            var containerProperties = new ContainerProperties(this.EventsContainerId[0], $"/{partitionKey}");
            var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);
            this.EventsContainers[EventsContainerId[0]] = EventDatabase.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughputProperties).GetAwaiter().GetResult()?.Container;
        }

        private void resetTimings()
        {
            requestCharge = 0;
            requestExecutionTime = 0;
        }

        public Events GetEvent(List<string> altNames, string scope)
        {
            resetTimings();

            // Find event with all possible event names
            foreach (var name in altNames)
            {
                try
                {
                    var eventsEntry = EventsContainers[EventsContainerId[0]].ReadItemAsync<Events>(name.ToLower(), new PartitionKey(scope.ToLower())).GetAwaiter().GetResult();
                    requestCharge += eventsEntry.RequestCharge;
                    requestExecutionTime += eventsEntry.Diagnostics.GetClientElapsedTime().TotalMilliseconds;

                    if (eventsEntry != null && eventsEntry.Resource.Status != Obsolete)
                    {
                        return eventsEntry.Resource;
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
                AssociatedPrerequisites= prereq,
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

        public double GetRequestCharge()
        {
            return requestCharge;
        }

        public double GetExecutionTime()
        {
            return requestExecutionTime;
        }

        public Tuple<double, double> UpdateEvent(Events record)
        {
            /* Update the new state */
            try
            {
                record.id = $"{record.EventName}".ToLower();
                record.pk = $"{record.Scope}".ToLower();
                var response = EventsContainers[EventsContainerId[0]].UpsertItemAsync<Events>(record).GetAwaiter().GetResult();
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
