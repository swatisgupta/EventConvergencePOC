using EventConvergencePOCTest.Contracts;
using EventConvergencPOCTest.Contracts;
using EventConvergencPOCTest.Src;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Security.AccessControl;
using Container = Microsoft.Azure.Cosmos.Container;

namespace EventConvergencePOCTest.Src
{
    public class EventsService
    {
        // will use EventDataSourceFactory
        CosmosClient cosmosClient1;
        CosmosClient cosmosClient2;
        string connectionString = "";

        CatalogDataSource catalogDataSource;
        EventsDataSource eventsDataSource;
        JobEventMappingsDataSource jobEventMappingsDataSource;
        Timer timer;

        int JobEventMappingsTimer = 0;
        int EventsTimer = 1;
        int CatalogTimer = 2;

        double JobEventMappingsRU = 0;
        double EventsRU = 0;
        double CatalogRU = 0;

        public EventsService()
        {
            timer = new Timer(3);
        }
        
        public void ResetStatistics()
        {
            timer.Reset();
            JobEventMappingsRU = 0;
            EventsRU = 0;
            CatalogRU = 0;
        }

        public void CreateConnection()
        {
            cosmosClient1 = new CosmosClient(connectionString,
                       new CosmosClientOptions()
                       {
                           AllowBulkExecution = true,
                       });
            cosmosClient2 = new CosmosClient(connectionString,
           new CosmosClientOptions()
           {
               AllowBulkExecution = true,
           });
            catalogDataSource = new CatalogDataSource(cosmosClient1);
            jobEventMappingsDataSource = new JobEventMappingsDataSource(cosmosClient2);
            eventsDataSource = new EventsDataSource(cosmosClient2, catalogDataSource, jobEventMappingsDataSource);

            catalogDataSource.GetCatalogContainer();
            eventsDataSource.GetEventsContainer();
            jobEventMappingsDataSource.GetJobEventContainers();
        }

        Events GetEventOldTable(string EventName, string scope)
        {
            // find event in system or other dataSourcefactory providers
            // Dummy methods for now
            return null;
        }

        Events CalculateState(Events entry1, Events entry2)
        {
            if(entry1 == null)
            {
                return entry2;
            }
            else if(entry2 == null)
            {
                return entry1;
            }
            else if(entry1.EventTimestamp > entry2.EventTimestamp)
            {
                return entry1;
            }
            else
            {
                return entry2;
            }
        }

        public void Register(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string,string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string, string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                if (jobRecord == null)
                {
                    jobRecord = new JobEventMappings
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        EventTimestamp = DateTime.UtcNow,
                        JobType = jobType,
                        Workflow = workflow,
                        BindingState = "Active",
                        Status = "Registered",
                    };
                }
                jobRecord.Status = "Registered";
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;
                // read Job-Event container if record found, update state to registered
                this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);
            }

            timer.EndTimer(JobEventMappingsTimer);
            // this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();

            timer.StartTimer();
            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var entry = CalculateState(oldEntry, newEntry);

            if (EventName.Equals(EntityName))
            {                 
                this.eventsDataSource.RegisterEvent(entry, EventName, scope, true, jobId, taskId, EventArguments, AdditionalScopes, jobType, workflow);
            }
            else
            {
                this.eventsDataSource.RegisterEvent(entry, entry != null ? entry.EventName : EventName, scope, true, jobId, taskId, EventArguments, AdditionalScopes, jobType, workflow); ;
            }
            timer.EndTimer(EventsTimer);
            // this.EventsRU += this.eventsDataSource.GetEventsRU();
            // this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();
        }

        public void Satisfy(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string,string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                // read Job-Event container if record found, update state to registered
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                if (jobRecord == null)
                {
                    jobRecord = new JobEventMappings
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        EventTimestamp = DateTime.UtcNow,
                        JobType = jobType,
                        Workflow = workflow,
                        BindingState = "Active",
                    };
                }
                string previousStatus = jobRecord.Status;

                jobRecord.Status = "Satisfied";
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;
                this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);
            }

            timer.EndTimer(JobEventMappingsTimer);
            this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();

            timer.StartTimer();
            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var entry = CalculateState(oldEntry, newEntry);

            if (EventName.Equals(EntityName))
            {
                this.eventsDataSource.SatisfyEvent(entry, EventName, scope, jobId, taskId, EventArguments, AdditionalScopes, jobType, workflow);
            }
            else
            {
                this.eventsDataSource.SatisfyEvent(entry, entry != null ? entry.EventName : EventName, scope, jobId, taskId, EventArguments, AdditionalScopes, jobType, workflow);
            }
            timer.EndTimer((int)EventsTimer);
            this.EventsRU += this.eventsDataSource.GetEventsRU();
            this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();

        }

        public void Cancel(string EventName, string scope, string jobId = null, string taskId = null)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = this.catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                // read Job-Event container if record found, update state to registered
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                if (jobRecord == null)
                {
                    jobRecord = new JobEventMappings
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        BindingState = "Active",
                    };
                }
                var previousStatus = jobRecord.Status;
                jobRecord.Status = "Canceled";
                jobRecord.EventTimestamp = DateTime.UtcNow;
                this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);
                timer.EndTimer(JobEventMappingsTimer);
                if(!previousStatus.Equals("Satisfied"))
                {
                    return;
                }
            }

            timer.EndTimer(JobEventMappingsTimer);
            this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();


            timer.StartTimer();
            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var entry = CalculateState(oldEntry, newEntry);

            if (EventName.Equals(EntityName))
            {
                this.eventsDataSource.CancelEvent(entry, EventName, altNames, scope, false, jobId, taskId);
            }
            else
            {
                this.eventsDataSource.CancelEvent(entry, entry != null ? entry.EventName : EventName, altNames, scope, false, jobId, taskId);
            }

            timer.EndTimer(EventsTimer);
            this.EventsRU += this.eventsDataSource.GetEventsRU();
            this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetJobEventMappingsRU();

        }


        public string GetExecutionTimes()
        {
            return $"{timer.GetTimer(CatalogTimer)} {timer.GetTimer(JobEventMappingsTimer)} {timer.GetTimer(EventsTimer)}";
        }

        public string GetRUConsumption()
        {
            return $"{CatalogRU} {JobEventMappingsRU} {EventsRU}";
        }
    }
}
