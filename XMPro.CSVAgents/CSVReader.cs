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

    public class CSVReader : IAgent, IReceivingAgent
    {
        private Configuration config;
        private string FilePathMapping => this.config["File"];
        private bool HasHeaderRecord => bool.Parse(this.config[nameof(HasHeaderRecord)]);
        private string Delimiter => this.config[nameof(Delimiter)];
        private string Encoding => this.config[nameof(Encoding)];
        private char Quote => this.config[nameof(Quote)].FirstOrDefault();
        private string OutputFileName => this.config[nameof(OutputFileName)];

        private CsvHelper.Configuration.Configuration csvConfig => new CsvHelper.Configuration.Configuration()
        {
            HasHeaderRecord = this.HasHeaderRecord,
            Delimiter = this.Delimiter,
            Encoding = System.Text.Encoding.GetEncoding(this.Encoding),
            Quote = this.Quote,
            ShouldSkipRecord = (row) => row.Length == 1 && row.First() == "\0",
        };

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        public event EventHandler<OnRequestParentOutputAttributesArgs> OnRequestParentOutputAttributes;

        private Grid _CSVDefinition;
        private IDictionary<string, Types> _fields = null;
        private IDictionary<string, Types> Fields
        {
            get
            {
                if (_fields == null)
                {
                    _fields = this.CSVDefinition.Rows.ToDictionary(
                        r => (string)r["Name"],
                        r => ((Types)Enum.Parse(typeof(Types), (string)r["Type"]))
                    );
                }
                return _fields;
            }
        }

        private List<XMIoT.Framework.Attribute> parentOutputs;

        private List<XMIoT.Framework.Attribute> ParentOutputs
        {
            get
            {
                if (parentOutputs == null)
                {
                    var args = new OnRequestParentOutputAttributesArgs(this.UniqueId, "Input");
                    this.OnRequestParentOutputAttributes.Invoke(this, args);
                    Console.WriteLine("Parent outputs Length:" + args.ParentOutputs.ToList().Count);
                    parentOutputs = args.ParentOutputs.ToList();
                }
                return parentOutputs;
            }
        }

        private Grid CSVDefinition
        {
            get
            {
                if (this._CSVDefinition == null)
                {
                    var grid = new Grid();
                    grid.Value = this.config["CSVDefinition"];
                    this._CSVDefinition = grid;
                }
                return this._CSVDefinition;
            }
        }

        public long UniqueId { get; set; }

        public CSVReader()
        {
            System.Text.Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        #region Agent Implementation

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            Settings settingsObj = Settings.Parse(template);
            new Populator(parameters).Populate(settingsObj);

#warning This should be taken care of via InputMappings
            DropDown CsvFile = settingsObj.Find("File") as DropDown;
            CsvFile.Options = this.ParentOutputs.Select(i => new Option() { DisplayMemeber = i.Name, ValueMemeber = i.Name }).ToList();

            Grid definition = (Grid)settingsObj.Find(nameof(CSVDefinition));
            CheckBox useFile = (CheckBox)settingsObj.Find("UseFileDefinition");
            FileUpload definitionFile = (FileUpload)settingsObj.Find("FileDef");
            definition.DisableDelete =
                definition.DisableInsert =
                definitionFile.Visible = useFile.Value;

            if (useFile.Value)
            {
                if (definitionFile.Value?.Content != null)
                {
                    string[] headers = null;
                    //Read file and retrieve headers
                    var csvHeaderConfig = new CsvHelper.Configuration.Configuration()
                    {
                        HasHeaderRecord = ((Setting<bool>)settingsObj.Find(nameof(HasHeaderRecord))).Value,
                        Delimiter = ((Setting<string>)settingsObj.Find(nameof(Delimiter))).Value,
                        Encoding = System.Text.Encoding.GetEncoding(((Setting<string>)settingsObj.Find(nameof(Encoding))).Value),
                        Quote = ((Setting<string>)settingsObj.Find(nameof(Quote))).Value.FirstOrDefault(),
                        ShouldSkipRecord = (row) => row.Length == 1 && row.First() == "\0",
                    };

                    using (var csv = new CsvReader(new StreamReader(new MemoryStream(definitionFile.Value.Content)), csvHeaderConfig))
                    {
                        csv.Read();
                        var h = csv.ReadHeader();
                        if (h == true)
                        {
                            headers = csv.Context.HeaderRecord;
                        }
                    }
                    if (headers != null)
                    {
                        IReadOnlyCollection<IDictionary<string, object>> fields = definition.Rows.ToArray();
                        //Put headers in the Definition grid; retaining values that are already there and removing those that do not have a matching header
                        var refreshedHeaders = headers
                            .Select(h => definition.Rows.FirstOrDefault(r => h == (string)r["Name"])
                                    ?? new Dictionary<string, object> { { "Name", h }, { "Type", nameof(Types.String) } });

                        definition.Value = Newtonsoft.Json.JsonConvert.SerializeObject(refreshedHeaders);
                    }
                }
                else
                {
                    //Clear the grid
                    definition.Value = Newtonsoft.Json.JsonConvert.SerializeObject(Array.Empty<IDictionary<string, object>>());
                }
            }

            return settingsObj.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(this.FilePathMapping))
                errors.Add("CSV File not selected");

            var CSVDefinitionGrid = new Grid();
            CSVDefinitionGrid.Value = this.config["CSVDefinition"];

            if (CSVDefinition.Rows.Any() == false)
                errors.Add("Payload should be defined");

            foreach (var row in CSVDefinitionGrid.Rows)
            {
                if (row["Name"] == null)
                    errors.Add($"Name Property not defined properly in payload.");

                if (row["Type"] == null)
                    errors.Add($"Type not defined for payload property {(string)row["Name"]}");

                if (string.IsNullOrWhiteSpace(this.OutputFileName) == false && (string)row["Name"] == this.OutputFileName)
                    errors.Add($"File name field \"{this.OutputFileName}\" overrides another field.");
            }

            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };

            //create a collection based on the user inputs
            return this.CSVDefinition.Rows.Select(r => new XMIoT.Framework.Attribute((string)r["Name"], ((Types)Enum.Parse(typeof(Types), (string)r["Type"]))))
                .Concat(string.IsNullOrWhiteSpace(this.OutputFileName) == false
                ? new XMIoT.Framework.Attribute[] { new XMIoT.Framework.Attribute(this.OutputFileName, Types.String) }
                : Array.Empty<XMIoT.Framework.Attribute>());
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;

            //Ensure Fields is initialised before it is needed
            this.Fields.Any();
        }

        public void Start()
        {
        }

        public void Destroy()
        {
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            return Array.Empty<XMIoT.Framework.Attribute>();
        }

        public void Receive(string endpointName, JArray events)
        {
            bool outputFileName = string.IsNullOrWhiteSpace(this.OutputFileName) == false;
            foreach (JObject _event in events)
            {
                if (_event[this.FilePathMapping] != null)
                {
                    var fileLoc = (string)_event[this.FilePathMapping];
                    var reader = new StreamReader(fileLoc, this.csvConfig.Encoding);
                    JArray records = new JArray();
                    using (var csv = new CsvReader(reader, this.csvConfig))
                    {
                        records = JArray.FromObject(csv.GetRecords<dynamic>());
                    }

                    foreach (JObject row in records)
                    {
                        foreach (var field in this.Fields)
                        {
                            if (!row.ContainsKey(field.Key))
                                row.Add(field.Key, null);
                            else
                                row.Property(field.Key).Value = convert(row.Property(field.Key).Value, field.Value);
                        }
                        if (outputFileName)
                            row.Add(this.OutputFileName, fileLoc);
                    }
                    this.OnPublish?.Invoke(this, new OnPublishArgs(records));
                }
            }
        }

        #endregion Agent Implementation

        #region Helper methods
        private static JToken convert(JToken value, Types type)
        {
            if (value.Type == JTokenType.Null)
                return null;

            //Prevents format exceptions in the case of empty strings
            if (value.Type == JTokenType.String && string.IsNullOrWhiteSpace((string)value) && type != Types.String)
                return null;

            switch (type)
            {
                case Types.String:
                    return value?.ToString();
                case Types.Long:
                    return Convert.ToInt64(value);
                case Types.Double:
                    return Convert.ToDouble(value);
                case Types.Boolean:
                    return Convert.ToBoolean(value);
                case Types.DateTime:
                    return Convert.ToDateTime(value);
                case Types.Int:
                    return Convert.ToInt32(value);
                default:
                    throw new Exception("Unknown conversion type");
            }
        }
        #endregion Helper methods
    }
}