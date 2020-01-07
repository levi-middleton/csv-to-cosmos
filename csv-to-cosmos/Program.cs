using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using Microsoft.Azure.Documents.Client;
using System.Configuration;

namespace csv_to_cosmos
{
    class Program
    {
        static void Main(string[] args)
        {
            var csvRows = new List<string[]>();
            //get csv data
            string csvFilename = ConfigurationManager.AppSettings["CsvFilename"];
            using (var csvParser = new TextFieldParser(csvFilename))
            {
                csvParser.SetDelimiters(",");
                csvParser.HasFieldsEnclosedInQuotes = true;

                csvParser.ReadLine(); //skip header row

                while (!csvParser.EndOfData)
                {
                    var fields = csvParser.ReadFields();
                    csvRows.Add(fields);
                }
            }

            Task.Run(async () =>
            {
                string endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
                string masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];
                string databaseId = ConfigurationManager.AppSettings["DocDbId"];
                string documentCollection = ConfigurationManager.AppSettings["DocDbCollection"];

                Uri documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, documentCollection);

                using (var client = new DocumentClient(new Uri(endpoint), masterKey))
                {
                    foreach (var row in csvRows)
                    {
                        dynamic documentDefinition = new
                        {
                            Module = row[0],
                            ComponentType = row[1],
                            Size = row[2],
                            SearchParameter = row[3],
                            SearchValue = row[4],
                            ReturnAttribute = row[5],
                            ReturnValue = row[6],
                            Key = string.Concat(row[0], "-", row[1], "-", row[5])
                        };

                        await client.CreateDocumentAsync(documentCollectionUri, documentDefinition);
                    }
                }
            }).Wait();

            Console.ReadLine();
        }
    }
}
