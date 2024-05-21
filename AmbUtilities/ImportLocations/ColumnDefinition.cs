namespace ImportLocations;

/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
internal class ColumnDefinition
{
    public readonly Settings.ColumnDefinition SettingsDefinition;
    public string Tag => SettingsDefinition.Tag;
    public string ColumnName => SettingsDefinition.Column;
    public bool Optional => SettingsDefinition.Optional;
    public bool MustExist => SettingsDefinition.MustExist;
    public readonly int ColumnNumber; 
    public ColumnDefinition? Parent = null;
    public ColumnDefinition? AliasOf = null;
    public GeographicLocation? AssignedGeographicLocation { get; set; }
    public string CurrentValue { get; set; } = "";
    public bool IsSystemOwned => SettingsDefinition.IsSystemOwned;

    public ColumnDefinition(Settings.ColumnDefinition columnDefinition, bool spreadsheetIsOneBased)
    {
        SettingsDefinition = columnDefinition;
        ColumnNumber = Program.ColumnAlphaToColumnNumber(SettingsDefinition.Column, spreadsheetIsOneBased);
    }

    public override string ToString()
    {
        return $"{ColumnName} {Tag}";
    }
}
