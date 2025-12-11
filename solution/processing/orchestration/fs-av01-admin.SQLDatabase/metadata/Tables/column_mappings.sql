CREATE TABLE [metadata].[column_mappings] (
    [mapping_id]    VARCHAR (100) NOT NULL,
    [column_order]  INT           NOT NULL,
    [source_column] VARCHAR (255) NOT NULL,
    [target_column] VARCHAR (100) NOT NULL,
    [data_type]     VARCHAR (50)  NOT NULL,
    [description]   VARCHAR (500) NULL,
    PRIMARY KEY CLUSTERED ([mapping_id] ASC, [column_order] ASC)
);


GO

