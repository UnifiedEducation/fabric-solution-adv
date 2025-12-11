CREATE TABLE [metadata].[loading_store] (
    [loading_id]      INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([loading_id] ASC)
);


GO

