using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace EventConvergencePOCTest.Src
{
    internal class EventsDataSource
    {
        CosmosClient cosmosClient;
        Database EventDatabase;
        Dictionary<string, Container> EventsContainers;
        string databaseId = "Events";
        List<string> EventsContainerId = new List<string> { "Events" };

        double RCEvents = 0;
        double RCCatalog = 0;
        double RCJobEventMappings = 0;

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
            RCCatalog = 0;
            RCEvents = 0;
            RCJobEventMappings = 0;
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
                    RCEvents += eventsEntry.RequestCharge;
     
                    if (eventsEntry != null && eventsEntry.Resource.Status != "Obsolete")
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

        public void RegisterEvent(Events oldState, string EventName, string scope, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, string jobType = null, string workflow = null)
        {
            resetTimings();
            bool isStateChanged = true;

            Events newState = new Events
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
                Status = "Registered"
            };

            if (oldState != null)
            {
                if (oldState.Status == "Satisfied")
                {
                    return;
                }
                // merge arguments??
            }

            if (isStateChanged)
            {
                UpdateEvent(oldState, newState);
            }
        }

        public void SatisfyEvent(Events oldState, string EventName, string scope, string jobId, string taskId, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, string jobType = null, string workflow = null)
        {
            resetTimings();
            bool isStateChanged = true;

            Events newState = new Events
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                AssociatedJobId = jobId,
                AssociatedTaskId = taskId,
                Arguments = EventArguments,
                AssociatedJobType = jobType,
                AssociatedWorkflow = workflow,
                Status = "Satisfied",
                AdditionalScopes = AdditionalScopes
            };

            if (isStateChanged)
            {
                UpdateEvent(oldState, newState);
            }
        }

        public void CancelEvent(Events oldState, string EventName, List<string> altNames, string scope, bool HardSet = false, string jobId = null, string taskId = null)
        {
            resetTimings();

            bool isStateChanged = true;

            Events newState = new Events
            {
                EventName = EventName,
                Scope = scope,
                EventTimestamp = DateTime.UtcNow,
                Status = "Canceled"
            };

            if (oldState != null)
            {
                List<JobEventMappings> jobEventMappings = null;

                if (oldState.Status == "Canceled")
                {
                    // Event already in canceled state - no need to update
                    isStateChanged = false;
                }

                if (HardSet)
                {
                    // Stateless operation requested through the Rest API call
                    newState.Status = "Canceled";
                }
                else if (oldState.Status == "Registered")
                {
                    jobEventMappings = this.jobEventMappingsDataSource.GetAllJobsRegisterEvent(altNames, scope);
                    RCJobEventMappings += this.jobEventMappingsDataSource.GetJobEventMappingsRU();
                }
                else if (oldState.Status == "Satisfied")
                {
                    jobEventMappings = this.jobEventMappingsDataSource.GetAllJobsEmitEvent(altNames, scope);
                    RCJobEventMappings += this.jobEventMappingsDataSource.GetJobEventMappingsRU();
                }

                if (jobEventMappings == null || jobEventMappings.Count == 0)
                {
                    // No jobs published or registered this event - cancel the event
                    newState.Status = "Canceled";
                }
                else if (oldState.AssociatedJobId.Equals(jobId) && oldState.AssociatedTaskId.Equals(taskId))
                {
                    // There are jobs that published or registered this event - but the job/task that satisfied the event is being canceled
                    // So we need to change the job association
                    newState.Status = oldState.Status;

                    // Find the last job that published or registered this event based on the event status
                    jobEventMappings?.Sort((x, y) => DateTime.Compare(x.EventTimestamp, y.EventTimestamp));
                    JobEventMappings JobData = jobEventMappings.LastOrDefault();

                    if (JobData != null)
                    {
                        newState.AssociatedJobId = JobData.JobId;
                        newState.AssociatedTaskId = JobData.TaskId;
                        newState.AssociatedJobType = JobData.JobType;
                        newState.AssociatedWorkflow = JobData.Workflow;
                        newState.Arguments = JobData.Arguments;
                        newState.AssociatedPrerequisites = JobData.AssociatedPrerequisites;
                        newState.AdditionalScopes = JobData.AdditionalScopes;
                    }
                }
                else
                {
                    // There are jobs that published or registered this event - and the job/task associated with the event is not being canceled
                    newState.Status = oldState.Status;
                    newState.AssociatedJobId = oldState.AssociatedJobId;
                    newState.AssociatedTaskId = oldState.AssociatedTaskId;
                    newState.AssociatedWorkflow = oldState.AssociatedWorkflow;
                    newState.Arguments = oldState.Arguments;
                    newState.AssociatedPrerequisites = oldState.AssociatedPrerequisites;
                    newState.AdditionalScopes = oldState.AdditionalScopes;
                }
            }

            if (isStateChanged)
            {
                UpdateEvent(oldState, newState);
            }
        }

        public double GetEventsRU()
        {
            return RCEvents;
        }

        public double GetJobEventMappingRU()
        {
            return this.RCJobEventMappings;
        }

        public double GetCatalogRU()
        {
            return this.RCCatalog;
        }

        private void UpdateEvent(Events oldState, Events newState)
        {
            /* Obsolete event when record exists with alias name and the request comes with new entity name */
            if (oldState != null && oldState.EventName != newState.EventName)
            {
                oldState.Status = "Obsolete";
                oldState.id = $"{oldState.EventName}".ToLower();
                oldState.pk = $"{oldState.Scope}".ToLower();
                try
                {
                    var response1 = EventsContainers[EventsContainerId[0]].UpsertItemAsync<Events>(oldState).GetAwaiter().GetResult();
                    RCEvents += response1.RequestCharge;
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    throw ex;
                }
            }

            /* Update the new state */
            try
            {
                newState.id = $"{newState.EventName}".ToLower();
                newState.pk = $"{newState.Scope}".ToLower();
                var response = EventsContainers[EventsContainerId[0]].UpsertItemAsync<Events>(newState).GetAwaiter().GetResult();
                RCEvents += response.RequestCharge;
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
        }
    }
}
