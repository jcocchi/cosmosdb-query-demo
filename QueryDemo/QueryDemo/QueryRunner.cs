using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace QueryFunctionalitySamples
{
    public class QueryRunner
    {
        private Container container;
        private string endpoint;

        public bool AccountWithImprovements { get; set; }

        public QueryRunner(string endpoint, string key, string database, string containerName)
        {
            this.endpoint = endpoint;

            var client = new CosmosClient(endpoint, key);
            container = client.GetDatabase(database).GetContainer(containerName);
        }

        public async Task<QueryStats> RunQueryAsync(string queryText)
        {
            var query = new QueryDefinition(queryText);

            PrintQuerySetup(queryText);

            var requestCharge = 0.0;
            var executionTime = new TimeSpan();
            var results = new List<dynamic>();

            var resultSetIterator = container.GetItemQueryIterator<dynamic>(query);
            while (resultSetIterator.HasMoreResults)
            {
                var response = await resultSetIterator.ReadNextAsync();
                results.AddRange(response.Resource);
                requestCharge += response.RequestCharge;
                executionTime += response.Diagnostics.GetClientElapsedTime();
            }

            Console.WriteLine($"Final Request charge: {requestCharge}, Final execution time: {executionTime}\n\n");

            var stats = new QueryStats()
            {
                RUCharge = requestCharge,
                ExecutionTime = executionTime
            };

            return stats;
        }

        public void PrintQuerySetup(string queryText)
        {
            if (AccountWithImprovements)
            {
                Console.WriteLine($"Running against account with improvements at {endpoint}");
            }
            else
            {
                Console.WriteLine($"Running against account without improvements at {endpoint}");
            }
            Console.WriteLine($"\t* Query: {queryText}\n");
        }
    }
}
