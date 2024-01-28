using EventConvergencePOCTest.Contracts;
using EventConvergencePOCTest.Src;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Tests
{
    public class LoadTestEventConvergence
    {
        static int MAX_ITERATIONS = 1;

        static int MAX_EVENTS = 10;

        static string resultPath = @"C:\Users\swatisinghal\OneDrive - Microsoft\Desktop\EventConvergencePOC\NoAlias";

        static List<string> EventNames;

        static string Scope = "Region.uswest";

        static long jobBase = 2518097332120667215;

        public enum Operation
        {
            Register,
            Satisfy,
            Cancel,
            Status,
            Item,
            SScope,
            Time,
            Name,
            JobId
        }

        static Event jobInput = new Event()
        {
            JobId = "_39df583a-48c8-4ce2-a4bb-20f0594ca5ec",
            TaskId = "6E7352D9-57A7-4451-B14B-EEA2D6ADDF74",
            JobType = "AzureFoundation",
            Workflow = "NewAZ",
            AdditionalScopes = new List<string> () 
            {
                "Cluster.LON22PrdApp10-02",
                "Cluster.SN1PrdApp11"
            },
            Arguments = new Dictionary<string, string> { { "SlbV2BuildPath", "\\\\exme.gbl.eaglex.ic.gov\\Builds\\branches\\git_networking_slb_hotfix_0_11_202_0" },
                                                         { "SlbV2BuildFqbn", "0.11.202.3" }, 
                                                         { "SlbV2MajorVersion", "0" },
                                                         { "SlbV2MinorVersion", "11" },
                                                         { "SlbV2RevisionVersion", "3" },
                                                         { "SlbV2DeploymentToolset", "usegit_networking_slb_hotfix_0_11_202_0=0.11.202.3" } },

            AssociatedPrerequisites = new List<Dictionary<string, string>>
                                    { new Dictionary<string, string> { { "Name", "DCMT.Config" },
                                        { "Scope", "Datacenter.LON22" },
                                        { "IsStaticEntity", "true" } },
                                       new Dictionary<string, string> { { "Name", "Scedo.Artifacts.Generated" },
                                        { "Scope", "DeploymentId.062920191" },
                                        { "IsStaticEntity", "true" } },
                                    },
        };

        // write a main method that will run all the tests
        public static void Main(String[] args)
        {
            LoadTestsParameters();

            RunTest(Operation.Register, "Register");
            RunTest(Operation.Satisfy, "Satisfy");
            RunTest(Operation.Cancel, "Cancel");
            RunTest(Operation.Status, "Status");
            RunTest(Operation.SScope, "SScope");
            RunTest(Operation.JobId, "Job");
            RunTest(Operation.Item, "Item");
            RunTest(Operation.Name, "Name");
            RunTest(Operation.Time, "Timestamp");
        }

        public static void LoadTestsParameters()
        {
            resultPath = @"C:\Users\swatisinghal\Documents\EventConvergencePOCNumbers\NoAlias";

            EventNames = new List<string>() { "Milestones.ESTS.InternallyReady",
                                                              "Milestones.ESTSGeo.InternallyReady",
                                                              "Milestones.ESTSGlobal.InternallyReady",
                                                              "Milestones.Gtos.InternallyReady",
                                                              "Milestones.SqlVM.InternallyReady",
                                                              "AZMilestones.AzNM.InternallyReady",
                                                              "Milestones.AadddsDpx.InternallyReady",
                                                              "Milestones.OneBranchRelease.InternallyReady",
                                                              "Services.Shoebox.InternallyReady",
                                                              "Milestones.RescuePP.InternallyReady" };
        }

        public static void RunTest(Operation op, string operation)
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();

            var scopes = new Dictionary<string, string>()
            {
                {  "Cluster", "LON22PrdApp10-02" },
                { "Cluster", "SN1PrdApp11" }
            };

            System.IO.Directory.CreateDirectory($"{resultPath}");
            System.IO.File.WriteAllLines($"{resultPath}\\{operation}Times.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\{operation}RU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\{operation}RUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\{operation}RUEvents.txt", test.GetRUPerDBOPerationEvents());
            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (int j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        jobIndex = jobIndex + 1 % 1000;
                        var jobId = $"{jobBase + jobIndex}{jobInput.JobId}";
                        var taskId = jobInput.TaskId;
                        switch (op)
                        {
                            case Operation.Register:
                                test.Register(EventNames[j], Scope, jobId, taskId,
                                      jobInput.Arguments, jobInput.AdditionalScopes, jobInput.AssociatedPrerequisites,
                                      jobInput.JobType, jobInput.Workflow);
                                break;
                            case Operation.Satisfy:
                                test.Satisfy(EventNames[j], Scope, jobId, taskId,
                                      jobInput.Arguments, jobInput.AdditionalScopes, jobInput.AssociatedPrerequisites,
                                      jobInput.JobType, jobInput.Workflow);
                                break;
                            case Operation.Cancel:
                                test.Cancel(EventNames[j], Scope, jobId, taskId);
                                break;
                            case Operation.Status:
                                test.GetStatus(EventNames[j], Scope);
                                break;
                            case Operation.SScope:
                                test.GetEventWithSecondaryScope(EventNames[j], scopes);
                                break;
                            case Operation.Time:
                                DateTime end = DateTime.Now;
                                DateTime start = end - new TimeSpan(0, 5, 0);
                                test.GetEventWithTimestamp(start, end);
                                break;
                            case Operation.Item:
                                test.GetEventWithPrimaryScope(EventNames[j], Scope);
                                break;
                            case Operation.Name:
                                test.GetEventsWithNamespace(EventNames[j]);
                                break;
                            case Operation.JobId:
                                test.GetEventsWithJob(jobId);
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Register: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Register: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\{operation}Times.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\{operation}RU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\{operation}RUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\{operation}RUEvents.txt", test.GetRUPerDBOPerationEvents());

            }
        }
    }
}
