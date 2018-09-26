# CSV-Listener
## Prerequisites
- Visual Studio (any version that supports .Net Core 2.1)
- [XMPro IoT Framework NuGet package](https://www.nuget.org/packages/XMPro.IOT.Framework/3.0.2-beta)
- Please see the [Building an Agent for XMPro IoT](https://docs.xmpro.com/lessons/writing-an-agent-for-xmpro-iot/) guide for a better understanding of how the XMPro IoT Framework works

## Description
The CSV listener agent allows the simulation of a stream using a CSV file.

## How the code works
All settings referred to in the code need to correspond with the settings defined in the template that has been created for the agent using the Stream Integration Manager. Refer to the [Stream Integration Manager](https://docs.xmpro.com/courses/packaging-an-agent-using-stream-integration-manager/) guide for instructions on how to define the settings in the template and package the agent after building the code. 

After packaging the agent, you can upload it to XMPro IoT and start using it.

### Settings
When a user needs to use the *CSV Listener*, they need to provide a CSV Definition. The CSV Definition grid lets a user specify the data type of each of the headers within the CSV file. Retrieve the value of this grid from the configuration by using the following code: 
```csharp
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
```

### Configurations
In the *GetConfigurationTemplate* method, parse the JSON representation of the settings into the Settings object.
```csharp
Settings settingsObj = Settings.Parse(template);
new Populator(parameters).Populate(settingsObj);
```

Get the setting for the file upload control that would allow the user to upload a CSV file and set the value. Get the content of the file and store it in a variable.
```csharp
FileUpload CsvFile = settingsObj.Find("File") as FileUpload;
CsvFile.Value = CsvFile.Value ?? new XMIoT.Framework.Settings.File();
var CsvFileContent = CsvFile.Value?.Content ?? null;
```

Get the CSV Definition grid setting from the settings object and enable delete and insert operations on the grid.
```csharp
var CSVDefinitionGrid = settingsObj.Find("CSVDefinition") as Grid;
CSVDefinitionGrid.DisableDelete = true;
CSVDefinitionGrid.DisableInsert = true;
```

If the CSV Definition grid is not empty, get all the headers of the CSV file. 
```csharp
var fields = new List<String>();
if (CsvFileContent != null)
    fields = GetHeader(CsvFileContent).ToList();
```

Next, for each of the headers, add a new row to the *JArray* object. For each of the children of this object, verify if there's an item that matches the *Name* key. If a match is found, set the *foundMatch* flag to *true*. If no match is found, add a new grid row containing the name and type of the field. By checking if a match can be found, you are making sure that duplicate headers are not added to the CSV Definition grid.

When the CSV Definition grid is populated with the headers of the CSV file, the default data type of all columns will be *String*. The user will then be responsible for making sure that the data type of these columns are correctly specified. If any data type column needs changing, the user has to update it.
```csharp
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
```

### Validate
When validating the CSV listener, make sure that a CSV file is uploaded. Otherwise, show an error.
```csharp
this.config = new Configuration() { Parameters = parameters };
var errors = new List<string>();

if (string.IsNullOrWhiteSpace(this.config["File"]))
    errors.Add("CSV File not selected");
```

### Create
Set the config variable to the configuration received in the *Create* method.
```csharp
this.config = configuration;
```
If the user uploaded a file, parse the file to be of type *XMIoT.Framework.Settings.File*. Read the file line by line, separating items by comma. Then, get the header/ column names of the CSV content (the header will always be the first line read from the file). For each line of the file that is read after getting the header, add each of the values for each of the columns to a dictionary after casting it to the correct type.
```csharp
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
```


### Start
There is no need to do anything in the *Start* method.

### Destroy
There is no need to do anything in the *Destroy* method.

### Publishing Events
Invoke the *OnPublish* event in the *Poll* method.
```csharp
public void Poll()
{
    rowCtr++;
    if (values.Count > 0 && rowCtr <= values.Count)//if records exist
    {                
        this.OnPublish?.Invoke(this, new OnPublishArgs(new JArray(values[rowCtr-1])));                
    }
}
```

### Getting Output Attributes
Set the configuration variable to a new instance of the Configuration class, providing the parameters received by the *GetOutputAttributes* method.
```csharp
this.config = new Configuration() { Parameters = parameters };
```

If rows have been added to the grid, create a collection based on the user inputs. Otherwise, get the file and parse it to an *XMIoT.Framework.Settings.File* object. Then, get the header of the CSV file and assume that all columns will be of type *String*.
```csharp
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
```

### Decrypting Values
This agent does not use any secure, automatically encrypted, values. There is no need to decrypt anything.

### Custom Methods
This agent uses three helper methods. The first method helps convert the value in a specific column and row in a CSV file into the correct format. This method is used in conjunction with the *GetSystemType* method, which returns the correct system type, given the string name of a data type.

```csharp
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
```

The third helper method gets the header of the content of a CSV file, which will always be the first row. The header is returned in the form of a string array after the string was split at each comma.
```csharp
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
```
