using CsvHelper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Linq;
using XMIoT.Framework;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Settings.Enums;
using System.IO;

namespace XMPro.CSVAgents
{
    public class CSVHeaderParser : IAgent, IReceivingAgent
    {
        private Configuration config;
        private string FileExtension => this.config["FileExtension"];
        private string OutputDirectory => string.IsNullOrWhiteSpace(this.config["OutputDirectory"]) ? null : this.config["OutputDirectory"];
        private string Delimiter => this.config["Delimiter"];
        private string Encoding => this.config["Encoding"];
        private char Quote => this.config["Quote"].FirstOrDefault();
        private bool QuoteAllFields => bool.Parse(this.config["QuoteAllFields"]);
        private bool QuoteNoFields => bool.Parse(this.config["QuoteNoFields"]);

        private CsvHelper.Configuration.Configuration csvConfig => new CsvHelper.Configuration.Configuration()
        {
            HasHeaderRecord = true,
            Delimiter = this.Delimiter,
            Encoding = System.Text.Encoding.GetEncoding(this.Encoding),
            Quote = this.Quote,
            QuoteAllFields = this.QuoteAllFields,
            QuoteNoFields = this.QuoteNoFields,
        };

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        public event EventHandler<OnRequestParentOutputAttributesArgs> OnRequestParentOutputAttributes;

        public long UniqueId { get; set; }

        public CSVHeaderParser()
        {
            System.Text.Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        #region Agent Implementation

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            Settings settingsObj = Settings.Parse(template);
            new Populator(parameters).Populate(settingsObj);

            return settingsObj.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            var errors = new List<string>();

            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            return new XMIoT.Framework.Attribute[]
            {
                new XMIoT.Framework.Attribute("FilePath", Types.String)
            };
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;
        }

        public void Start()
        {
        }

        public void Destroy()
        {
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            return new XMIoT.Framework.Attribute[]
            {
                new XMIoT.Framework.Attribute("InputPath", Types.String)
            };
        }

        public void Receive(string endpointName, JArray events)
        {
            JArray output = new JArray();
            foreach (JObject _event in events)
            {
                string inputPath = (string)_event["InputPath"];

                string outputPath = Path.Combine((this.OutputDirectory ?? Path.GetDirectoryName(inputPath)), Path.GetFileNameWithoutExtension(inputPath) + this.FileExtension);
                using (var fileWriter = new StreamWriter(outputPath, false, csvConfig.Encoding))
                using (var fileReader = new StreamReader(inputPath, csvConfig.Encoding))
                using (var csvWriter = new CsvWriter(fileWriter, csvConfig))
                using (var csvReader = new CsvParser(fileReader, csvConfig))
                {
                    foreach(string fieldName in ParseHeader(csvReader))
                    {
                        csvWriter.WriteField(fieldName);
                    }

                    csvWriter.NextRecord();

                    foreach (string[] row in ParseBody(csvReader))
                    {
                        foreach (string field in row)
                        {
                            csvWriter.WriteField(field);
                        }
                        csvWriter.NextRecord();
                    }
                    csvWriter.Flush();
                }
                var record = new JObject();
                record.Add("FilePath", outputPath);
                output.Add(record);
            }

            this.OnPublish(this, new OnPublishArgs(output));

            string[] ParseHeader(CsvParser parser)
            {
                string[] header = parser.Read();
                string prev = null;
                for (int i = 0; i < header.Length; i++)
                {
                    string curr = header[i];
                    if (prev != null && curr == "*")
                    {
                        header[i] = curr = prev + " Flag";
                    }
                    prev = curr;
                }

                return header;
            }

            IEnumerable<string[]> ParseBody(CsvParser parser)
            {
                while (parser.Read() is string[] record && record.Length > 0)
                {
                    if (record.Length == 1 && record.First() == "\0")
                        yield break;
                    else
                        yield return record;
                }
            }
        }

        #endregion Agent Implementation
    }
}