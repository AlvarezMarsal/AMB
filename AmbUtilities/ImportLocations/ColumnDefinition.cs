namespace ImportLocations;

internal class ColumnDefinition
{
    public readonly Settings.ColumnDefinition SettingsDefinition;
    public string Tag => SettingsDefinition.Tag;
    public string ColumnName => SettingsDefinition.Column;
    public readonly int ColumnNumber; 
    public readonly List<ColumnDefinition> Parents = []; // I assume it's possible that an entity can have more than one parent -- for example, a city and a zipcode
    public readonly List<ColumnDefinition> AliasedBy = [];
    public GeographicLocation? AssignedGeographicLocation { get; set; }
    public string CurrentValue { get; set; } = "";

    public ColumnDefinition(Settings.ColumnDefinition columnDefinition, bool spreadsheetIsOneBased)
    {
        SettingsDefinition = columnDefinition;
        ColumnNumber = Program.ColumnAlphaToColumnNumber(SettingsDefinition.Column, spreadsheetIsOneBased);
    }
}
