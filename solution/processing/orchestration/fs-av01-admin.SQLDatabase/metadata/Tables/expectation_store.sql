CREATE TABLE [metadata].[expectation_store] (
    [expectation_id]   INT            NOT NULL,
    [expectation_name] VARCHAR (100)  NOT NULL,
    [gx_method]        VARCHAR (100)  NOT NULL,
    [description]      VARCHAR (1000) NULL,
    [expected_params]  JSON           NULL,
    PRIMARY KEY CLUSTERED ([expectation_id] ASC)
);


GO

