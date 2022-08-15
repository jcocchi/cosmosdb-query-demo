using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace QueryFunctionalitySamples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var database = configuration["Database"];
            var container = configuration["Container"];
            var runnerWithImprovements = new QueryRunner(configuration["EndpointWithImprovements"], configuration["KeyWithImprovements"], database, container)
            {
                AccountWithImprovements = true
            };
            var runnerWithoutImprovements = new QueryRunner(configuration["EndpointWithoutImprovements"], configuration["KeyWithoutImprovements"], database, container)
            {
                AccountWithImprovements = false
            };

            await RunDemoMenu(runnerWithImprovements, runnerWithoutImprovements);
        }

        public static async Task RunDemoMenu(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            bool exit = false;

            while (exit == false)
            {
                Console.Clear();
                Console.WriteLine($"Azure Cosmos DB Query Optimizations Demo");
                Console.WriteLine($"-----------------------------------------------------------");
                Console.WriteLine($"[1]   GROUP BY using the index");
                Console.WriteLine($"[2]   DISTINCT using the index");
                Console.WriteLine($"[3]   DateTimeBin system function");
                Console.WriteLine($"[4]   OFFSET LIMIT optimizations");
                Console.WriteLine($"[5]   JOIN optimizations");
                Console.WriteLine($"[6]   EXISTS optimizations");
                Console.WriteLine($"[7]   Exit\n");

                ConsoleKeyInfo result = Console.ReadKey(true);

                if (result.KeyChar == '1')
                {
                    await RunGroupByDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '2')
                {
                    await RunDistinctDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '3')
                {
                    await RunDateTimeBinDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '4')
                {
                    await RunOffsetLimitDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '5')
                {
                    await RunJoinDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '6')
                {
                    await RunExistsDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '7')
                {
                    Console.WriteLine("Goodbye!");
                    exit = true;
                }
            }
        }

        public static async Task RunGroupByDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing improvements to GROUP BY which can now use the index.");
            Console.WriteLine($"-----------------------------------------------------------");

            var groupByText = "SELECT Count(1), c.Category FROM c GROUP BY c.Category";

            var statsWithout = await runnerWithout.RunQueryAsync(groupByText);
            var statsWith = await runnerWith.RunQueryAsync(groupByText);

            PrintComparisonOutput(statsWith, statsWithout, groupByText);

            var groupByText2 = "SELECT AVG(c.Price), c.Category FROM c GROUP BY c.Category";

            var statsWithout2 = await runnerWithout.RunQueryAsync(groupByText2);
            var statsWith2 = await runnerWith.RunQueryAsync(groupByText2);

            PrintComparisonOutput(statsWith2, statsWithout2, groupByText2);
        }

        public static async Task RunDistinctDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing improvements to DISTINCT which can now use the index.");
            Console.WriteLine($"-----------------------------------------------------------");

            var distinctText = "SELECT DISTINCT c.Name FROM c WHERE c.Price > 500";

            var statsWithout = await runnerWithout.RunQueryAsync(distinctText);
            var statsWith = await runnerWith.RunQueryAsync(distinctText);

            PrintComparisonOutput(statsWith, statsWithout, distinctText);
        }

        public static async Task RunDateTimeBinDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing the new DateTimeBin system function.");
            Console.WriteLine($"-----------------------------------------------------------");

            var dateTimeBinText = "SELECT Count(1) as NewProduts, DateTimeBin(c.FirstAvailable, 'd', 7) AS DayAvailable FROM c WHERE c.FirstAvailable > \"2022-06-01T00:00:00.0000000Z\" GROUP BY DateTimeBin(c.FirstAvailable, 'd', 7)";

            var statsWithout = await runnerWithout.RunQueryAsync(dateTimeBinText);
            var statsWith = await runnerWith.RunQueryAsync(dateTimeBinText);

            PrintComparisonOutput(statsWith, statsWithout, dateTimeBinText);

            var dateTimeBinText2 = "SELECT Count(1) as NewProduts, DateTimeBin(c.FirstAvailable,'d', 7, \"2022-06-01T00:00:00.0000000Z\") AS DayAvailable FROM c  WHERE c.FirstAvailable > \"2022-06-01T00:00:00.0000000Z\" GROUP BY DateTimeBin(c.FirstAvailable, 'd', 7, \"2022-06-01T00:00:00.0000000Z\")";

            var statsWithout2 = await runnerWithout.RunQueryAsync(dateTimeBinText2);
            var statsWith2 = await runnerWith.RunQueryAsync(dateTimeBinText2);

            PrintComparisonOutput(statsWith2, statsWithout2, dateTimeBinText2);
        }

        public static async Task RunOffsetLimitDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing improvements to OFFSET LIMIT.");
            Console.WriteLine($"-----------------------------------------------------------");

            var offsetLimitText = "SELECT c.Name, c.Price, c.FirstAvailable FROM c WHERE c.Price > 150 AND c.Price < 500 ORDER BY c.FirstAvailable DESC OFFSET 1000 LIMIT 100";

            var statsWithout = await runnerWithout.RunQueryAsync(offsetLimitText);
            var statsWith = await runnerWith.RunQueryAsync(offsetLimitText);

            PrintComparisonOutput(statsWith, statsWithout, offsetLimitText);

            var offsetLimitText2 = "SELECT c.Name, c.Price, c.FirstAvailable FROM c WHERE c.Price > 150 AND c.Price < 500 ORDER BY c.FirstAvailable DESC OFFSET 100000 LIMIT 100";

            var statsWithout2 = await runnerWithout.RunQueryAsync(offsetLimitText2);
            var statsWith2 = await runnerWith.RunQueryAsync(offsetLimitText2);

            PrintComparisonOutput(statsWith2, statsWithout2, offsetLimitText2);
        }

        public static async Task RunJoinDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing improvements to JOIN without needing to write subqueries.");
            Console.WriteLine($"-----------------------------------------------------------");

            var joinSubqueryText = "SELECT c.Name, c.Price, Rating FROM c JOIN(SELECT VALUE r FROM r IN c.CustomerRatings WHERE r.Stars < 3) AS Rating WHERE c.Price > 950";

            var statsWithout = await runnerWithout.RunQueryAsync(joinSubqueryText);
            var statsWith = await runnerWith.RunQueryAsync(joinSubqueryText);

            PrintComparisonOutput(statsWith, statsWithout, joinSubqueryText);

            var joinText = "SELECT c.Name, c.Price, r as Rating FROM c JOIN r IN c.CustomerRatings WHERE c.Price > 950 and r.Stars < 3";

            var statsWithout2 = await runnerWithout.RunQueryAsync(joinText);
            var statsWith2 = await runnerWith.RunQueryAsync(joinText);

            PrintComparisonOutput(statsWith2, statsWithout2, joinText);
        }

        public static async Task RunExistsDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing improvements to EXISTS.");
            Console.WriteLine($"-----------------------------------------------------------");

            var existsText = "SELECT Count(1) FROM c WHERE c.Category = \"Music\" and EXISTS(SELECT VALUE r FROM r IN c.CustomerRatings WHERE r.Stars > 4)";

            var statsWithout = await runnerWithout.RunQueryAsync(existsText);
            var statsWith = await runnerWith.RunQueryAsync(existsText);

            PrintComparisonOutput(statsWith, statsWithout, existsText);
        }

        private static void PrintComparisonOutput(QueryStats statsWith, QueryStats statsWithout, string queryText)
        {
            Console.WriteLine($"\nShowing final results for query \"{queryText}\"");
            Console.WriteLine($"-----------------------------------------------------------");

            Console.WriteLine("|Account             |RU Charge |Execution Time  |");
            Console.WriteLine("|--------------------|----------|----------------|");
            Console.WriteLine("|Before improvements |{0, -10}|{1, -16}|", Math.Round(statsWithout.RUCharge, 2), statsWithout.ExecutionTime);
            Console.WriteLine("|After improvements  |{0, -10}|{1, -16}|", Math.Round(statsWith.RUCharge, 2), statsWith.ExecutionTime);

            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
            Console.WriteLine();
        }
    }
}
