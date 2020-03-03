using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using CsvHelper;
using System.Globalization;

namespace csv_to_cosmos
{
    class Program
    {
        static void Main(string[] args)
        {
            //get csv data
            string csvFilename = ConfigurationManager.AppSettings["CsvFilename"];

            var items = new List<Dictionary<string, string>>();
            using (var stream = new FileStream(csvFilename, FileMode.Open))
            using (var reader = new StreamReader(stream))
            using (var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csvReader.Read();
                csvReader.ReadHeader();
                string[] headerRow = csvReader.Context.HeaderRecord;

                while (csvReader.Read())
                {
                    var dict = new Dictionary<string, string>();
                    foreach (string column in headerRow)
                    {
                        if (string.IsNullOrEmpty(column))
                            continue;
                        string value = csvReader.GetField(column);
                        if (string.IsNullOrEmpty(value))
                            continue;
                        dict.Add(column.ToLower().Trim(), value.ToLower().Trim());
                    }
                    items.Add(dict);
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
                    foreach (var row in items)
                    {
                        row.Add("Key", string.Concat("-", row["module"], "-", row["returnattribute"]));

                        await client.CreateDocumentAsync(documentCollectionUri, row);
                    }
                }
            }).Wait();

            Console.ReadLine();
        }
    }
}
