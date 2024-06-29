USE [GeoNames]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Entity](
	[GeoNameId] [bigint] NOT NULL,
	[AsciiName] [nvarchar](200) NOT NULL,
	[FeatureClass] [nchar](1) NOT NULL,
	[FeatureCode] [nvarchar](10) NOT NULL,
	[CountryCode] [nchar](2) NOT NULL,
	[Admin1Code] [nvarchar](20) NOT NULL,
	[Admin2Code] [nvarchar](80) NOT NULL,
	[Admin3Code] [nvarchar](20) NOT NULL,
	[Population] [bigint] NOT NULL,
	[BenchmarkId] [bigint] NOT NULL,
	[ParentGeoNameId] [bigint] NOT NULL,
	[Continent] [nchar](2) NOT NULL,

 CONSTRAINT [PK_Entity] PRIMARY KEY CLUSTERED 
(
	[GeoNameId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[EntityExtra](
	[GeoNameId] [bigint] NOT NULL,
	[Name] [nvarchar](200) NOT NULL,
	[AlternateNames] [nvarchar](max) NOT NULL,
	[Latitude] [float] NOT NULL,
	[Longitude] [float] NOT NULL,
	[CC2] [nvarchar](200) NOT NULL,
	[Admin4Code] [nvarchar](20) NOT NULL,
	[Admin5Code] [nvarchar](20) NOT NULL,
	[Elevation] [int] NOT NULL,
	[Dem] [nvarchar](20) NOT NULL,
	[Timezone] [nvarchar](40) NOT NULL,
	[ModificationDate] [datetime] NOT NULL,
	[LineNumber] [int] NOT NULL,
 CONSTRAINT [PK_EntityExtra] PRIMARY KEY CLUSTERED 
(
	[GeoNameId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[AlternateName](
	[AlternateNameId] [bigint] NULL,
	[GeoNameId] [bigint] NOT NULL,
	[Language] [nvarchar](7) NOT NULL,
	[AlternateName] [nvarchar](400) NOT NULL,
 CONSTRAINT [PK_AlternateName] PRIMARY KEY CLUSTERED 
(
	[GeoNameId] ASC,
	[AlternateName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

CREATE TABLE [dbo].[AlternateNameExtra](
	[AlternateNameId] [bigint] NOT NULL,
	[IsPreferredName] [bit] NOT NULL,
	[IsShortName] [bit] NOT NULL,
	[IsColloquial] [bit] NOT NULL,
	[IsHistoric] [bit] NOT NULL,
	[From] [datetime] NOT NULL,
	[To] [datetime] NOT NULL,
	[LineNumber] [int] NOT NULL
 CONSTRAINT [PK_AlternateNameExtra] PRIMARY KEY CLUSTERED 
(
	[AlternateNameId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE OR ALTER PROCEDURE [dbo].[InsertEntity]
    @GeoNameId bigint,
    @Name nvarchar(200),
    @AsciiName  nvarchar(200),
    @AlternateNames  nvarchar(MAX),
    @Latitude  float,
    @Longitude  float,
    @FeatureClass  nchar,
    @FeatureCode  nvarchar(10),
    @CountryCode  nchar(2),
    @CC2  nvarchar(200),
    @Admin1Code  nvarchar(20),
    @Admin2Code  nvarchar(80),
    @Admin3Code  nvarchar(20),
    @Admin4Code  nvarchar(20),
    @Admin5Code  nvarchar(20),
    @Population  bigint,
    @Elevation  int,
    @Dem  nvarchar(20),
    @Timezone  nvarchar(40),
    @ModificationDate  datetime,
    @Continent  nchar(2),
    @LineNumber int,
	@ParentGeoNameId bigint = 0
AS
BEGIN
    IF NOT EXISTS (SELECT * FROM [dbo].[Entity] WHERE [GeoNameId] = @GeoNameId)
    BEGIN

        INSERT INTO [dbo].[Entity] (
            [GeoNameId], [AsciiName], [FeatureClass], [FeatureCode], 
            [CountryCode], [Admin1Code], [Admin2Code], [Admin3Code], [Population],
            [BenchmarkId], [ParentGeoNameId]
        ) VALUES (
            @GeoNameId , @AsciiName, @FeatureClass, @FeatureCode, 
            @CountryCode, @Admin1Code, @Admin2Code, @Admin3Code, @Population,
            0, @ParentGeoNameId
        )

        INSERT INTO [dbo].[EntityExtra] (
            [GeoNameId], [Name], [AlternateNames], [Latitude], [Longitude],
            [CC2], [Admin4Code], [Admin5Code], [Elevation], 
            [Dem], [Timezone], [ModificationDate], [LineNumber]
        ) VALUES (
            @GeoNameId, @Name, @AlternateNames, @Latitude, @Longitude,
            @CC2, @Admin4Code, @Admin5Code, @Elevation, 
            @Dem, @Timezone, @ModificationDate, @LineNumber
        )

		DECLARE @A bigint
		SELECT @A = MIN([AlternateNameId]) FROM [dbo].[AlternateName]
		IF (@A IS NULL)
			SET @A = -1
		ELSE
			SET @A = @A-1
		INSERT INTO [dbo].[AlternateName] (
			[AlternateNameId], [GeoNameId], [Language], [AlternateName]
		) VALUES (
			@A, @GeoNameId, N'', @AsciiName
		)

		INSERT INTO [dbo].[AlternateNameExtra] (
			[AlternateNameId], [IsPreferredName], [IsShortName], [IsColloquial], [IsHistoric], [From], [To], [LineNumber]
		) VALUES (
			@A, 1, 0, 0, 0, '1800-01-01', '2999-01-01', @LineNumber
		)
    END
	ELSE
	BEGIN
		UPDATE [dbo].[EntityExtra] SET
			[LineNumber] = @LineNumber
		WHERE [GeoNameId] = @GeoNameId
	END
END            
GO

CREATE OR ALTER PROCEDURE [dbo].[InsertAlternateName]
    @AlternateNameId bigint = 0,
    @GeoNameId bigint,
    @Language nvarchar(7),
    @AlternateName nvarchar(400),
    @IsPreferredName bit,
    @IsShortName bit,
    @IsColloquial bit,
    @IsHistoric bit,
    @From datetime,
    @To datetime,
    @LineNumber int
AS
BEGIN
    IF NOT EXISTS (SELECT * FROM [dbo].[AlternateName] WHERE [GeoNameId] = @GeoNameId AND [AlternateName] = @AlternateName)
    BEGIN

		IF (@AlternateNameId = 0)
		BEGIN
			DECLARE @A bigint
			SELECT @A = MIN(AlternateNameId) FROM [dbo].[AlternateName]
			IF (@A IS NULL)
				SET @AlternateName = -1
			ELSE
				SET @AlternateNameId = @A-1
		END


		INSERT INTO [dbo].[AlternateName] (
            [AlternateNameId], [GeoNameId], [Language], [AlternateName]
        ) VALUES (
            @AlternateNameId, @GeoNameId, @Language, @AlternateName
        )

		INSERT INTO [dbo].[AlternateNameExtra] (
            [AlternateNameId], [IsPreferredName], [IsShortName], [IsColloquial], [IsHistoric], [From], [To], [LineNumber]
        ) VALUES (
            @AlternateNameId, @IsPreferredName, @IsShortName, @IsColloquial,@IsHistoric, @From, @To, @LineNumber
        )

    END
	ELSE
	BEGIN
		UPDATE [dbo].[AlternateNameExtra]
		SET [LineNumber] = @LineNumber
		WHERE [AlternateNameId] = (SELECT TOP(1) [AlternateNameId] FROM [dbo].[AlternateName] WHERE [GeoNameId] = @GeoNameId)
	END
END
GO

USE [GeoNames]
GO

/****** Object:  StoredProcedure [dbo].[InsertEntity]    Script Date: 06/28/2024 10:38:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER   PROCEDURE [dbo].[InsertGeographicLocation]
	@BenchmarkId BIGINT NULL,
	@ParentId BIGINT NULL,
	@Name NVARCHAR(512),
	@CreationDate DATETIME2,
	@CreatorId INT,
	@PracticeAreaId INT,
	@CreationSession UNIQUEIDENTIFIER
AS
BEGIN

	IF NOT EXISTS (SELECT * FROM [AMBenchmark_DB].[dbo].[t_GeographicLocation] WHERE [OID] = @BenchmarkId)
	BEGIN

		DECLARE @Index INT

		SELECT @Index = MAX([Index]) FROM [AMBenchmark_DB].[dbo].[t_GeographicLocation] WHERE [PID] = @ParentId
		IF @Index IS NULL
			SET @Index = 0
		ELSE
			SET @Index = @Index + 1

		INSERT INTO [AMBenchmark_DB].[dbo].[t_GeographicLocation]
			([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorID], [PracticeAreaID] ,[CreationSession])
		VALUES
			(@BenchmarkId, @ParentId, 1, @Name, @Index, @Name, @CreationDate, @CreatorId, @PracticeAreaId, @CreationSession)

	END

	UPDATE [GeoNames].[dbo].[Entity] SET [BenchmarkId] = @BenchmarkId

	IF NOT EXISTS (SELECT * FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [Alias] = @Name)
	BEGIN

		DECLARE @AliasOid BIGINT
		DECLARE @AliasCount INT
		DECLARE @IsPrimary BIT

		EXEC [AMBenchmark_DB].[dbo].[sp_internalGetNextOID] @AliasOid OUTPUT
		SET @AliasCount = (SELECT COUNT(*) FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [IsPrimary] = 1)

		IF (@AliasCount = 0)
			SET @IsPrimary = 1
		ELSE
			SET @IsPrimary = 0

		INSERT INTO [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias]
			([OID], [Alias], [Description], [IsSystemOwned], [IsPrimary], [CreationDate], [CreatorID], [PracticeAreaID] ,[GeographicLocationID], [LID], [CreationSession])
		VALUES
			(@AliasOid, @Name, @Name, 1, @IsPrimary, @CreationDate, @CreatorId, @PracticeAreaId, @BenchmarkId, 500, @CreationSession)

	END
	ELSE
	BEGIN
		UPDATE [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias]
			SET Alias = ''
			WHERE OID = -1
	END

END
GO


CREATE OR ALTER   PROCEDURE [dbo].[AssignCountryToContinent]
    @Country nchar(2),
    @Continent nchar(2)
AS
BEGIN
	DECLARE @C INT
	DECLARE @ID BIGINT

	SELECT @C = COUNT(E.GeoNameId) 
	FROM [GeoNames].[dbo].[AlternateName] N
	JOIN [GeoNames].[dbo].[Entity] E ON E.[GeoNameId] = N.[GeoNameId]
	WHERE N.[AlternateName] = @Country AND E.[FeatureCode] = 'COUNTRY'

	IF @C = 1 
	BEGIN
		SELECT @ID = E.GeoNameId
		FROM [GeoNames].[dbo].[AlternateName] N
		JOIN [GeoNames].[dbo].[Entity] E ON E.[GeoNameId] = N.[GeoNameId]
		WHERE N.[AlternateName] = @Country AND E.[FeatureCode] = 'COUNTRY'

		UPDATE [GeoNames].[dbo].[Entity] 
		SET [Continent] = @Continent
		WHERE [GeoNameId] = @ID
	END

END
GO

USE [GeoNames]
GO



CREATE OR ALTER   PROCEDURE [dbo].[AssignCountryToContinent]
    @Country nchar(2),
    @Continent nchar(2)
AS
BEGIN
	DECLARE @C INT
	DECLARE @ID BIGINT

	SELECT @C = COUNT(E.GeoNameId) 
	FROM [GeoNames].[dbo].[AlternateName] N
	JOIN [GeoNames].[dbo].[Entity] E ON E.[GeoNameId] = N.[GeoNameId]
	WHERE N.[AlternateName] = @Country AND E.[FeatureCode] = 'COUNTRY'

	IF @C = 1 
	BEGIN
		SELECT @ID = E.GeoNameId
		FROM [GeoNames].[dbo].[AlternateName] N
		JOIN [GeoNames].[dbo].[Entity] E ON E.[GeoNameId] = N.[GeoNameId]
		WHERE N.[AlternateName] = @Country AND E.[FeatureCode] = 'COUNTRY'

		UPDATE [GeoNames].[dbo].[Entity] 
		SET [Continent] = @Continent
		WHERE [GeoNameId] = @ID
	END

END
GO



CREATE OR ALTER       PROCEDURE [dbo].[InsertGeographicLocation]
	@GeoNameId BIGINT,
	@BenchmarkId BIGINT OUTPUT,
	@ParentId BIGINT,
	@Name NVARCHAR(512),
	@CreationDate DATETIME2,
	@CreatorId INT,
	@PracticeAreaId INT,
	@CreationSession UNIQUEIDENTIFIER
AS
BEGIN
	DECLARE @ExistingOid BIGINT
	DECLARE @Index INT

	-- See if Benchmark already defines this country (specifically, if there's an alias defined matching the Parent and Name)
	SELECT @ExistingOid = G.[OID]
		FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] A
		JOIN [AMBenchmark_DB].[dbo].[t_GeographicLocation] G ON G.OID = A.GeographicLocationId
		WHERE G.PID = @ParentId AND [Alias] = @Name

	IF @ExistingOid IS NULL
	BEGIN
		IF @BenchmarkId = 0
			EXEC [AMBenchmark_DB].[dbo].[sp_internalGetNextOID] @BenchmarkId OUTPUT

		SELECT @Index = MAX([Index]) FROM [AMBenchmark_DB].[dbo].[t_GeographicLocation] WHERE [PID] = @ParentId
		IF @Index IS NULL
			SET @Index = 0
		ELSE
			SET @Index = @Index + 1

		INSERT INTO [AMBenchmark_DB].[dbo].[t_GeographicLocation]
           ([OID],
            [PID],
            [IsSystemOwned],
            [Name],
            [Index],
            [Description],
            [CreationDate],
            [CreatorID],
            [PracticeAreaID],
            [CreationSession])
     VALUES
           (@BenchmarkId,
            @ParentId,
            1,
            @Name,
		    @Index,
		    @Name,
		    @CreationDate,
		    @CreatorId,
		    @PracticeAreaId,
		    @CreationSession)

	END
	ELSE -- it DOES already exist
	BEGIN
		SET @BenchmarkId = @ExistingOid
	END

	UPDATE [GeoNames].[dbo].[Entity]
		SET [BenchmarkId] = @BenchmarkId
		WHERE [GeoNameId] = @GeoNameId

	IF NOT EXISTS (SELECT [Alias] FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [Alias] = @Name)
	BEGIN

		DECLARE @AliasOid BIGINT
		DECLARE @AliasCount INT
		DECLARE @IsPrimary BIT

		EXEC [AMBenchmark_DB].[dbo].[sp_internalGetNextOID] @AliasOid OUTPUT
		SET @AliasCount = (SELECT COUNT(*) FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [IsPrimary] = 1)

		IF (@AliasCount = 0)
			SET @IsPrimary = 1
		ELSE
			SET @IsPrimary = 0

		INSERT INTO [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias]
			([OID], [Alias], [Description], [IsSystemOwned], [IsPrimary], [CreationDate], [CreatorID], [PracticeAreaID] ,[GeographicLocationID], [LID], [CreationSession])
		VALUES
			(@AliasOid, @Name, @Name, 1, @IsPrimary, @CreationDate, @CreatorId, @PracticeAreaId, @BenchmarkId, 500, @CreationSession)

	END

END
GO

USE [GeoNames]
GO

USE [GeoNames]
GO

/****** Object:  StoredProcedure [dbo].[InsertGeographicLocationAlias]    Script Date: 06/29/2024 14:41:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE OR ALTER       PROCEDURE [dbo].[InsertGeographicLocationAlias]
	@BenchmarkId BIGINT,
	@Name NVARCHAR(512),
	@CreationDate DATETIME2,
	@CreatorId INT,
	@PracticeAreaId INT,
	@CreationSession UNIQUEIDENTIFIER
AS
BEGIN

	IF NOT EXISTS (SELECT [Alias] FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [Alias] = @Name)
	BEGIN

		DECLARE @AliasOid BIGINT
		DECLARE @AliasCount INT
		DECLARE @IsPrimary BIT

		EXEC [AMBenchmark_DB].[dbo].[sp_internalGetNextOID] @AliasOid OUTPUT
		SET @AliasCount = (SELECT COUNT(*) FROM [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = @BenchmarkId AND [IsPrimary] = 1)

		IF (@AliasCount = 0)
			SET @IsPrimary = 1
		ELSE
			SET @IsPrimary = 0

		INSERT INTO [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias]
			([OID], [Alias], [Description], [IsSystemOwned], [IsPrimary], [CreationDate], [CreatorID], [PracticeAreaID] ,[GeographicLocationID], [LID], [CreationSession])
		VALUES
			(@AliasOid, @Name, @Name, 1, @IsPrimary, @CreationDate, @CreatorId, @PracticeAreaId, @BenchmarkId, 500, @CreationSession)
	END
	ELSE
	BEGIN
		UPDATE [AMBenchmark_DB].[dbo].[t_GeographicLocationAlias] 
			SET OID = 0
			WHERE OID = -1
	END

END
GO


