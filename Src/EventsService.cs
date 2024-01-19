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
using System.Threading.Tasks;
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

        public void Register(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string, string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            /*
            Task<Tuple<double, double>>[] TaskList =    {
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); } ),
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); }),
                                                            new Task<Tuple<double, double>>(() => { return new Tuple<double, double>( 0, 0 ); }),
                                                         };
            */

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            CatalogRU += catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetRequestCharge();

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
                        BindingState = JobEventMappingsDataSource.BindingStateActive,
                    };
                }
                jobRecord.Status = JobEventMappingsDataSource.Registered;
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;
                // read Job-Event container if record found, update state to registered
                var res = jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);

                this.JobEventMappingsRU += res.Item1;

                // TaskList[0] = new Task<Tuple<double, double>>(() => jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord));
            }

            /* 
             TaskList[0].Start();
            TaskList[0].Wait();
            */

            timer.EndTimer(JobEventMappingsTimer);

            timer.StartTimer();
            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry);
            bool isStatusChanged = false;
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            isStatusChanged = this.eventsDataSource.RegisterEvent(existingState, out Events newState, targetName, scope, true, jobId, taskId, EventArguments, AdditionalScopes, AssociatedPrerequisites, jobType, workflow);

            if (isStatusChanged)
            {
                if (existingState != null && existingState.EventName != targetName)
                {
                    existingState.Status = EventsDataSource.Obsolete;
                    // TaskList[1] = new Task<Tuple<double, double>>(() => this.eventsDataSource.UpdateEvent(existingState));
                   var res =  this.eventsDataSource.UpdateEvent(existingState);

                    this.EventsRU += res.Item1;
                    // timer.UpdateTimer(EventsTimer, TaskList[1].Result.Item2 + TaskList[2].Result.Item2);
                }
                var res2 = this.eventsDataSource.UpdateEvent(newState);
                this.EventsRU += res2.Item1;

            }
            /*
            TaskList[1].Start();
            TaskList[2].Start();
            */

            timer.EndTimer((int)EventsTimer);
            this.EventsRU += this.eventsDataSource.GetRequestCharge();
           /*
            Task.WaitAll(TaskList);

            this.JobEventMappingsRU += TaskList[0].Result.Item1;
            timer.UpdateTimer(JobEventMappingsTimer, TaskList[0].Result.Item2);

            this.EventsRU += TaskList[1].Result.Item1 + TaskList[2].Result.Item1;
            timer.UpdateTimer(EventsTimer, TaskList[1].Result.Item2 + TaskList[2].Result.Item2);
           */
        }

        public void Satisfy(string EventName, string scope, string jobId = null, string taskId = null, Dictionary<string, string> EventArguments = null, List<Dictionary<string, string>> AdditionalScopes = null, List<Dictionary<string,string>> AssociatedPrerequisites = null, string jobType = null, string workflow = null)
        {
            ResetStatistics();

            /*
            Task<Tuple<double, double>>[] TaskList =    {
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); } ),
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); }),
                                                            new Task<Tuple<double, double>>(() => { return new Tuple<double, double>( 0, 0 ); }),
                                                         };
            */

            timer.StartTimer();
            var altNames = catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            if (jobId != null && taskId != null)
            {
                // read Job-Event container if record found, update state to registered
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetRequestCharge();

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
                        BindingState = JobEventMappingsDataSource.BindingStateActive,
                    };
                }
                string previousStatus = jobRecord.Status;

                jobRecord.Status = JobEventMappingsDataSource.Satisfied;
                jobRecord.EventTimestamp = DateTime.UtcNow;
                jobRecord.Arguments = EventArguments ?? jobRecord.Arguments;
                jobRecord.AdditionalScopes = AdditionalScopes ?? jobRecord.AdditionalScopes;
                jobRecord.AssociatedPrerequisites = AssociatedPrerequisites ?? jobRecord.AssociatedPrerequisites;
                // TaskList[0] = new Task<Tuple<double, double>>(() => this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord));
                var res = this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);
                JobEventMappingsRU += res.Item1;
            }
            // TaskList[0].Start();
            // TaskList[0].Wait();
            timer.EndTimer(JobEventMappingsTimer);

            timer.StartTimer();

            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            var isStatusChanged = this.eventsDataSource.SatisfyEvent(existingState, out Events newState, targetName, scope, jobId, taskId, EventArguments, AdditionalScopes, AssociatedPrerequisites, jobType, workflow);

            if (isStatusChanged)
            {
                if (existingState != null && existingState.EventName != targetName)
                {
                    existingState.Status = EventsDataSource.Obsolete;
                    //TaskList[1] = new Task<Tuple<double, double>>(() => this.eventsDataSource.UpdateEvent(existingState));
                    var res1 = this.eventsDataSource.UpdateEvent(existingState);
                    this.EventsRU += res1.Item1;

                }
                // TaskList[2] = new Task<Tuple<double, double>>(() => this.eventsDataSource.UpdateEvent(newState));
                var res2 = this.eventsDataSource.UpdateEvent(newState);
                this.EventsRU += res2.Item1;

            }
            // TaskList[1].Start();
            // TaskList[2].Start();
            timer.EndTimer(EventsTimer);
            this.EventsRU += this.eventsDataSource.GetRequestCharge();
            /*
            Task.WaitAll(TaskList);

            this.JobEventMappingsRU += TaskList[0].Result.Item1;
            timer.UpdateTimer(JobEventMappingsTimer, TaskList[0].Result.Item2);

            this.EventsRU += TaskList[1].Result.Item1 + TaskList[2].Result.Item1;
            timer.UpdateTimer(EventsTimer, TaskList[1].Result.Item2 + TaskList[2].Result.Item2); */

        }

        public void Cancel(string EventName, string scope, string jobId = null, string taskId = null)
        {
            ResetStatistics();

            /*
            Task<Tuple<double, double>>[] TaskList =    {
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); } ),
                                                            new Task<Tuple<double, double>>( () => { return new Tuple<double, double>( 0, 0 ); }),
                                                            new Task<Tuple<double, double>>(() => { return new Tuple<double, double>( 0, 0 ); }),
                                                         };
            */

            timer.StartTimer();
            var altNames = this.catalogDataSource.GetCatalogEntity(EventName, out string EntityName);
            timer.EndTimer(CatalogTimer);
            this.CatalogRU += this.catalogDataSource.GetCatalogRU();

            timer.StartTimer();
            
            if (jobId != null && taskId != null)
            {
                // read Job-Event container if record found, update state to registered
                var jobRecord = this.jobEventMappingsDataSource.GetEventForJob(jobId, taskId, altNames, scope);
                this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetRequestCharge();

                if (jobRecord == null)
                {
                    jobRecord = new JobEventMappings
                    {
                        PublishingEvent = EventName,
                        Scope = scope,
                        JobId = jobId,
                        TaskId = taskId,
                        BindingState = JobEventMappingsDataSource.BindingStateActive,
                    };
                }
                var previousStatus = jobRecord.Status;
                jobRecord.Status = JobEventMappingsDataSource.Canceled;
                jobRecord.EventTimestamp = DateTime.UtcNow;

                // TaskList[0] = new Task<Tuple<double, double>>(() => this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord));
                var res = this.jobEventMappingsDataSource.UpdateJobEventMappings(jobRecord);
                this.JobEventMappingsRU += res.Item1;
                if (previousStatus == null || !previousStatus.Equals(JobEventMappingsDataSource.Satisfied))
                {
                    // TaskList[0].Start();
                    // Task.WaitAll(TaskList[0]);
                    timer.EndTimer(JobEventMappingsTimer);
                    return;
                }
            }
            // TaskList[0].Start();
            // TaskList[0].Wait();

            timer.EndTimer(JobEventMappingsTimer);


            timer.StartTimer();
            var newEntry = this.eventsDataSource.GetEvent(altNames, scope);
            var oldEntry = GetEventOldTable(EventName, scope);

            var existingState = CalculateState(oldEntry, newEntry);
            var targetName = EventName.Equals(EntityName) ? EntityName : existingState != null ? existingState.EventName : EventName;

            var isStatusChanged = this.eventsDataSource.CancelEvent(existingState, out Events newState, targetName, altNames, scope, false, jobId, taskId);


            if (isStatusChanged)
            {
                if (existingState != null && existingState.EventName != targetName)
                {
                    existingState.Status = EventsDataSource.Obsolete;
                    // TaskList[1] = new Task<Tuple<double, double>>(() => this.eventsDataSource.UpdateEvent(existingState));
                   var res = this.eventsDataSource.UpdateEvent(existingState);
                   this.EventsRU += res.Item1;

                }
                var res1 = this.eventsDataSource.UpdateEvent(newState);
                this.EventsRU += res1.Item1;

            }
            timer.EndTimer((int)EventsTimer);
            this.EventsRU += this.eventsDataSource.GetRequestCharge();
            this.JobEventMappingsRU += this.jobEventMappingsDataSource.GetRequestCharge();
            /*
            Task.WaitAll(TaskList);

            this.JobEventMappingsRU += TaskList[0].Result.Item1;
            timer.UpdateTimer(JobEventMappingsTimer, TaskList[0].Result.Item2);

            this.EventsRU += TaskList[1].Result.Item1 + TaskList[2].Result.Item1;
            timer.UpdateTimer(EventsTimer, TaskList[1].Result.Item2 + TaskList[2].Result.Item2);
            */
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
