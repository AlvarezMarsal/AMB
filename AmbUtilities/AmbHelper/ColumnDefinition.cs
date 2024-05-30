namespace AmbHelper;

/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class ColumnDefinition<T>
{
    public readonly Settings.Column SettingsDefinition;
    public string Tag => SettingsDefinition.Tag;
    public string ColumnName => SettingsDefinition.Name;
    public bool Optional => SettingsDefinition.Optional;
    public bool MustExist => SettingsDefinition.MustExist;
    public readonly int ColumnNumber; 
    public ColumnDefinition<T>? Parent = null;
    public ColumnDefinition<T>? AliasOf = null;
    public T? AssignedValue { get; set; }
    public string CurrentValue { get; set; } = "";
    public bool IsSystemOwned => SettingsDefinition.IsSystemOwned;
    public List<string> Exclusions => SettingsDefinition.ExcludedValues;

    public ColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
    {
        SettingsDefinition = columnDefinition;
        ColumnNumber = ExcelHelper.ColumnAlphaToColumnNumber(SettingsDefinition.Name, spreadsheetIsOneBased);
    }

    public override string ToString()
    {
        return $"{ColumnName} {Tag} '{CurrentValue}'";
    }
}
