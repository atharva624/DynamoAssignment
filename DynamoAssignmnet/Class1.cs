using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FinalDataFilterNodePOC
{
    public static class CsvDataProcessor
    {
        public static Dictionary<string, Dictionary<string, int>> ProcessCsvData(string csvFilePath)
        {
            if (string.IsNullOrWhiteSpace(csvFilePath) || !File.Exists(csvFilePath))
            {
                throw new ArgumentException("Invalid file path or file does not exist.");
            }

            var csvContent = File.ReadAllLines(csvFilePath)
                                 .Select(line => line.Split(',').ToList())
                                 .ToList();

            if (csvContent == null || csvContent.Count < 2)
            {
                throw new ArgumentException("CSV data must contain a header row and at least one data row.");
            }

            int operationColumn = 0;  // First column: Operation
            int timeColumn = 1;       // Second column: Time
            int eventsColumn = 2;     // Third column: Events
            int exchangeColumn = 3;   // Fourth column: ExchangeName
            int modelColumn = 4;      // Fifth column: ModelName

            var parsedData = csvContent.Skip(1) // Skip the header row
                .Select(row => new ExchangeRecord
                {
                    Operation = row[operationColumn],
                    Time = int.TryParse(row[timeColumn], out var time) ? time : 0,
                    Events = int.TryParse(row[eventsColumn], out var events) ? events : 0,
                    ExchangeName = row[exchangeColumn],
                    ModelName = row[modelColumn],
                })
                .ToList();

            var filteredData = parsedData.Where(record => record.Operation == "UpdateExchangeAsync" ||
                                                           record.Operation == "UpdateExchangeAsync:GenerateViewableAsync");

            var aggregatedData = from record in filteredData
                                 group record by new { record.ModelName, record.ExchangeName } into groupedRecords
                                 select new
                                 {
                                     groupedRecords.Key.ModelName,
                                     groupedRecords.Key.ExchangeName,
                                     TotalTime = groupedRecords.Sum(g => g.Time)
                                 };

            var result = new Dictionary<string, Dictionary<string, int>>();

            foreach (var record in aggregatedData)
            {
                if (!result.ContainsKey(record.ModelName))
                {
                    result[record.ModelName] = new Dictionary<string, int>();
                }

                result[record.ModelName][record.ExchangeName] = record.TotalTime;
            }

            return result;
        }
    }

  
    public class ExchangeRecord
    {
        public string Operation { get; set; }
        public int Time { get; set; }
        public int Events { get; set; }
        public string ExchangeName { get; set; }
        public string ModelName { get; set; }
        public int ExecutionTime { get; set; }

        public override string ToString()
        {
            return $"ModelName: {ModelName}, ExchangeName: {ExchangeName}, Time: {Time}";
        }
    }
}
