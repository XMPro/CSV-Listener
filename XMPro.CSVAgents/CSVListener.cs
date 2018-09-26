using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using XMIoT.Framework;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Settings.Enums;

namespace XMPro.CSVAgents
{
    public class Listener : IAgent, IPollingAgent
    {
        private Configuration config;
        private string[] header;
        private JArray values = new JArray();
        private Grid _CSVDefinition;
        private int rowCtr = 0;

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

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

        #region Agent Implementation

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            Settings settingsObj = Settings.Parse(template);
            new Populator(parameters).Populate(settingsObj);

            FileUpload CsvFile = settingsObj.Find("File") as FileUpload;
            CsvFile.Value = CsvFile.Value ?? new XMIoT.Framework.Settings.File();
            var CsvFileContent = CsvFile.Value?.Content ?? null;

            var CSVDefinitionGrid = settingsObj.Find("CSVDefinition") as Grid;
            CSVDefinitionGrid.DisableDelete = true;
            CSVDefinitionGrid.DisableInsert = true;

            if (CSVDefinitionGrid != null)
            {
                var fields = new List<String>();
                if (CsvFileContent != null)
                    fields = GetHeader(CsvFileContent).ToList();

                var newRows = new JArray();
                var rows = CSVDefinitionGrid.Rows?.ToList() ?? new List<IDictionary<string, object>>();
                foreach (var row in rows)
                {
                    if (fields.Contains(row["Name"].ToString()) == true)
                        newRows.Add(JObject.FromObject(row));
                }

                foreach (var field in fields)
                {
                    bool foundMatch = false;
                    foreach (JObject row in newRows.Children<JObject>())
                    {
                        var d = row.ToObject<Dictionary<string, object>>();
                        if (d.ContainsKey("Name") && d["Name"].ToString() == field)
                        {
                            foundMatch = true;
                            break;
                        }
                    }

                    if (!foundMatch)
                    {
                        var newRow = new JObject();
                        newRow.Add("Name", field);
                        newRow.Add("Type", "String");
                        var idx = fields.IndexOf(field) < newRows.Count ? fields.IndexOf(field) : newRows.Count;
                        newRows.Insert(idx, newRow);
                    }
                }
                CSVDefinitionGrid.Value = newRows.ToString();
            }

            return settingsObj.ToString();
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(this.config["File"]))
                errors.Add("CSV File not selected");

            return errors.ToArray();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };

            if (this.CSVDefinition.Rows.Count() > 0)
            {
                //create a collection based on the user inputs
                return this.CSVDefinition.Rows.Select(r => new XMIoT.Framework.Attribute(r["Name"].ToString(), ((Types)Enum.Parse(typeof(Types), r["Type"].ToString()))));
            }
            else if (!string.IsNullOrWhiteSpace(this.config["File"]))//if csv file has been uploaded
            {
                var file = XMIoT.Framework.Settings.File.Parse(this.config["File"]);
                //get the header of the csv and assume all columns to be of string type
                return GetHeader(file.Content).Select(c => new XMIoT.Framework.Attribute(c, Types.String));
            }
            else
                return new XMIoT.Framework.Attribute[0].AsEnumerable();
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;

            var line = "";
            header = new string[0];
            if (!string.IsNullOrWhiteSpace(this.config["File"]))
            {
                var file = XMIoT.Framework.Settings.File.Parse(this.config["File"]);
                int ctr = 0;
                using (var reader = new StreamReader(new MemoryStream(file.Content)))
                {
                    while (reader != null && !reader.EndOfStream)
                    {
                        line = reader.ReadLine();
                        if (ctr == 0)
                            header = line.Split(',');
                        else
                        {
                            var row = line.Split(',');
                            var rowObj = new JObject();
                            for (int i = 0; i < header.Length; i++)//for each column add a value into a dictionary after casting it to proper type
                            {
                                rowObj.Add(new JProperty(header[i], convert(header[i], row[i])));
                            }
                            values.Add(rowObj);
                        }
                        ctr++;
                    }
                }
            }
            else
                throw new FileNotFoundException();
        }

        public void Start()
        {            
        }

        public void Destroy()
        {            
        }

        public void Poll()
        {
            rowCtr++;
            if (values.Count > 0 && rowCtr <= values.Count)//if records exist
            {                
                this.OnPublish?.Invoke(this, new OnPublishArgs(new JArray(values[rowCtr-1])));                
            }
        }

        #endregion Agent Implementation

        #region Helper Methods

        private object convert(string name, string value)
        {
            foreach (var dr in this.CSVDefinition.Rows)
            {
                if (name.Equals(dr["Name"]?.ToString(), StringComparison.InvariantCultureIgnoreCase))
                {
                    TypeConverter typeConverter = TypeDescriptor.GetConverter(GetSystemType(dr["Type"].ToString()));
                    return typeConverter.ConvertFromInvariantString(value);
                }
            }
            return value;
        }

        private Type GetSystemType(string type)
        {
            switch (type)
            {
                case "Boolean":
                    return typeof(System.Boolean);
                case "Long":
                    return typeof(System.Int64);
                case "Int":
                    return typeof(System.Int32);
                case "DateTime":
                    return typeof(System.DateTime);
                case "Double":
                    return typeof(System.Double);
                default:
                    return typeof(System.String);
            }
        }

        private string[] GetHeader(byte[] file)
        {
            string[] columns;
            using (var reader = new StreamReader(new MemoryStream(file)))
            {
                var line = "";
                if (!reader.EndOfStream)
                {
                    line = reader.ReadLine();
                    columns = line.Split(',');
                }
                else
                    columns = new string[0];
            }

            return columns;
        }

        #endregion Helper Methods
    }
}