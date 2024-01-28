using EventConvergencePOCTest.Contracts;
using EventConvergencPOCTest.Contracts;
using EventConvergencPOCTest.Src;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Permissions;
using System.Threading.Tasks;
using System.Web;
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
        EventSummaryDataSource eventSummaryDataSource;
        EventDataSource eventDataSource;
        Timer timer;

        int EventTimer = 0;
        int EventSummaryTimer = 1;
        int CatalogTimer = 2;

        double EventRU = 0;
        double EventSummaryRU = 0;
        double CatalogRU = 0;

        public EventsService()
        {
            timer = new Timer(3);
        }
        
        public void ResetStatistics()
        {
            timer.Reset();
            EventRU = 0;
            EventSummaryRU = 0;
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
            eventDataSource = new EventDataSource(cosmosClient2);
            eventSummaryDataSource = new EventSummaryDataSource(cosmosClient2, catalogDataSource, eventDataSource);

            catalogDataSource.GetCatalogContainer();
            eventSummaryDataSource.GetEventSummaryContainer();
            eventDataSource.GetEventContainers();
        }

        EventSummary GetEventOldTable(string EventName, string scope)
        {
            // find event in system or other dataSourcefactory providers
            // Dummy methods for now
            return null;
        }

        EventSummary CalculateState(EventSummary entry1, EventSummary entry2)
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

        public void UpdateEventSummary(string EventName, string scope, string Action, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<string> AdditionalScopes = null, List<Dictionary<string, string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            CatalogRU += catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                var jobRecord = eventDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                EventRU += eventDataSource.GetRequestCharge();

                if (jobRecord == null)
                {
                    jobRecord = new Event
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        EventTimestamp = DateTime.UtcNow,
                        JobType = jobType,
                        Workflow = workflow,
                        BindingState = EventDataSource.BindingStateActive,
                    };
                }
                jobRecord.Action = EventDataSource.Registered;
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;

                // read Event container if record found, update state to registered
                var res = eventDataSource.UpdateEvent(jobRecord);

                EventRU += res.Item1;
            }

            timer.EndTimer(EventTimer);

            timer.StartTimer();


            var newEntry = eventSummaryDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry?.payload);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            bool isStatusChanged = eventSummaryDataSource.RegisterEvent(existingState, out EventSummary newState, targetName, scope, true, jobId, taskId, EventArguments, AdditionalScopes, AssociatedPrerequisites, jobType, workflow);
            var cosmosItem = new CosmosDbItem<EventSummary>();

            if (isStatusChanged)
            {
                var res2 = eventSummaryDataSource.UpdateEvent(newState, newEntry.etag);
                EventSummaryRU += res2.Item1;
            }

            timer.EndTimer(EventSummaryTimer);
            EventRU += eventDataSource.GetRequestCharge();
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();

        }

        public void Register(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<string> AdditionalScopes = null, List<Dictionary<string, string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            CatalogRU += catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                var jobRecord = eventDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                EventRU += eventDataSource.GetRequestCharge();

                if (jobRecord == null)
                {
                    jobRecord = new Event
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        EventTimestamp = DateTime.UtcNow,
                        JobType = jobType,
                        Workflow = workflow,
                        BindingState = EventDataSource.BindingStateActive,
                    };
                }
                jobRecord.Action = EventDataSource.Registered;
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;

                // read Event container if record found, update state to registered
                var res = eventDataSource.UpdateEvent(jobRecord);

                EventRU += res.Item1;
            }

            timer.EndTimer(EventTimer);

            timer.StartTimer();

            
            var newEntry = eventSummaryDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry?.payload);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            bool isStatusChanged = eventSummaryDataSource.RegisterEvent(existingState, out EventSummary newState, targetName, scope, true, jobId, taskId, EventArguments, AdditionalScopes, AssociatedPrerequisites, jobType, workflow);
            var cosmosItem = new CosmosDbItem<EventSummary>();

            if (isStatusChanged)
            {
                var res2 = eventSummaryDataSource.UpdateEvent(newState, newEntry.etag);
                EventSummaryRU += res2.Item1;

            }

            timer.EndTimer(EventSummaryTimer);
            EventRU += eventDataSource.GetRequestCharge();
           EventSummaryRU += eventSummaryDataSource.GetRequestCharge();

        }

        public void Satisfy(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<string> AdditionalScopes = null, List<Dictionary<string,string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
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
                var jobRecord = eventDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                EventRU += eventDataSource.GetRequestCharge();

                if (jobRecord == null)
                {
                    jobRecord = new Event
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        EventTimestamp = DateTime.UtcNow,
                        JobType = jobType,
                        Workflow = workflow,
                        BindingState = EventDataSource.BindingStateActive,
                    };
                }
                jobRecord.Action = EventDataSource.Satisfied;
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;
                var res = eventDataSource.UpdateEvent(jobRecord);
                EventRU += res.Item1;
            }
            timer.EndTimer(EventTimer);

            timer.StartTimer();

            var newEntry = eventSummaryDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry?.payload);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            var isStatusChanged = eventSummaryDataSource.SatisfyEvent(existingState, out EventSummary newState, targetName, scope, jobId, taskId, EventArguments, AdditionalScopes, AssociatedPrerequisites, jobType, workflow);


            if (isStatusChanged)
            {
                var res2 = eventSummaryDataSource.UpdateEvent(newState, newEntry.etag);
                EventSummaryRU += res2.Item1;

            }
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();

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
                var jobRecord = eventDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                EventSummaryRU += eventSummaryDataSource.GetRequestCharge();

                if (jobRecord == null)
                {
                    jobRecord = new Event
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        BindingState = EventDataSource.BindingStateActive,
                    };
                }

                jobRecord.Action = EventDataSource.Canceled;
                jobRecord.EventTimestamp = DateTime.UtcNow;

                var res = eventDataSource.UpdateEvent(jobRecord);
                EventRU += res.Item1;
            }

            timer.EndTimer(EventTimer);


            timer.StartTimer();

             var newEntry = eventSummaryDataSource.GetEvent(altNames, scope);
               var oldEntry = GetEventOldTable(EventName, scope);

               var existingState = CalculateState(oldEntry, newEntry?.payload);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            var isStatusChanged = eventSummaryDataSource.CancelEvent(existingState, out EventSummary newState, targetName, altNames, scope, false, jobId, taskId);

            if (isStatusChanged)
            {
                var res2 = eventSummaryDataSource.UpdateEvent(newState, newEntry.etag);
                EventSummaryRU += res2.Item1;

            }

            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();
        }

        public string GetStatus(string EventName, string scope)
        {
            ResetStatistics();

            timer.StartTimer();
            var altNames = this.catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            var eventRecord = eventSummaryDataSource.GetEvent(altNames, scope);
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            var status = "NotAvailable";

            if(eventRecord != null)
            {
                status = eventRecord.payload?.Status;
            }
            return status;
        }

        public EventSummary GetEventWithPrimaryScope(string EventName, string scope)
        {
            EventSummary eventRecord = null;
            ResetStatistics();

            timer.StartTimer();
            var altNames = this.catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            eventRecord = eventSummaryDataSource.GetEvent(altNames, scope)?.payload;
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            return eventRecord;
        }


        public EventSummary GetEventWithSecondaryScope(string EventName, Dictionary<string, string> scopes)
        {
            EventSummary eventRecord = null;
            ResetStatistics();

            timer.StartTimer();
            var altNames = this.catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            List<string> scopeValues = new List<string>();

            foreach (var scope in scopes)
            {
                scopeValues.Add($"{scope.Key}.{scope.Value}");
            }

            QueryDefinition query = new QueryDefinition(
                "select * from t where t.pk in @entity and t.AdditionalScopes.Name in @scopes")
                .WithParameter("@entity", altNames)
                .WithParameter("@scopes", scopeValues);

            eventRecord = eventSummaryDataSource.GetEventByQuery(query.QueryText)?.FirstOrDefault();
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            return eventRecord;
        }

        public List<EventSummary> GetEventWithTimestamp(DateTime startTime, DateTime endTime)
        {
            List<EventSummary> eventRecord = null;
            ResetStatistics();

            timer.StartTimer();
            List<string> scopeValues = new List<string>();

            QueryDefinition query = new QueryDefinition(
                "select * from t where t.EventTimestamp > @start and t.EventTimestamp <= @end")
                .WithParameter("@start", startTime)
                .WithParameter("@end", endTime);

            eventRecord = eventSummaryDataSource.GetEventByQuery(query.QueryText);
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            return eventRecord;
        }

        public List<EventSummary> GetEventsWithNamespace(string namespaceVal)
        {
            List<EventSummary> eventRecord = null;
            ResetStatistics();

            timer.StartTimer();
            List<string> scopeValues = new List<string>();

            QueryDefinition query = new QueryDefinition(
                "select * from t where t.EventName == @name")
                .WithParameter("@name", namespaceVal);

            eventRecord = eventSummaryDataSource.GetEventByQuery(query.QueryText);
            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            return eventRecord;
        }

        public List<EventSummary> GetEventsWithJob(string jobId)
        {
            List<EventSummary> eventRecord = new List<EventSummary>();
            ResetStatistics();

            timer.StartTimer();
            List<string> scopeValues = new List<string>();

            QueryDefinition query = new QueryDefinition(
                "select * from t where t.JobId == @name")
                .WithParameter("@name", jobId);

            var eventsRecord = eventDataSource.GetEventForQuery(query.QueryText);

            foreach(var ev in eventsRecord)
            {
                var evS = GetEventWithPrimaryScope(ev.PublishingEvent, ev.Scope);
                if(evS != null)
                {
                    eventRecord.Add(evS);
                }
            }

            timer.EndTimer(EventSummaryTimer);
            EventSummaryRU += eventSummaryDataSource.GetRequestCharge();
            EventRU += eventDataSource.GetRequestCharge();

            return eventRecord;
        }



        public string GetExecutionTimes()
        {
            return $"{timer.GetTimer(CatalogTimer)} {timer.GetTimer(EventTimer)} {timer.GetTimer(EventSummaryTimer)}";
        }

        public string GetRUConsumption()
        {
            return $"{CatalogRU} {EventRU} {EventSummaryRU}";
        }

        public void ClearRU()
        {
            eventDataSource.ClearCharge();
            eventSummaryDataSource.ClearCharge();
        }
        public List<string> GetRUPerDBOPerationMappings()
        {
            return eventDataSource.GetRUPerDBoperation();
        }


        public List<string> GetRUPerDBOPerationEvents()
        {
            return eventSummaryDataSource.GetRUPerDBoperation();
        }
    }
}
