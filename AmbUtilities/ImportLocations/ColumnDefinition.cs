namespace ImportLocations;

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
