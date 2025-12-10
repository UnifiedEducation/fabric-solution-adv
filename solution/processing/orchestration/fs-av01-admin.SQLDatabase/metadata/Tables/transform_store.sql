CREATE TABLE [metadata].[transform_store] (
    [transform_id]    INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([transform_id] ASC)
);


GO

