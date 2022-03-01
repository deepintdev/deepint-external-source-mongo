# Deep Intelligence External source: Mongo DB

This is an external source to connect a Mongo DB collection to a Deep Intelligence.

## Installation

Install depedendencies

```
$ npm install
```

Requirements:

 - Node JS

To build the project type:

```
$ npm run build
```

To run the server type:

```
$ npm start
```

## Configuration

In order to configure this module, you have to set the following environment variables:

| Variable Name | Description |
|---|---|
| HTTP_PORT | HTTP listening port. Default is `80` |
| SSL_PORT | HTTPS listening port. Default is `443` |
| SSL_CERT | Path to SSL certificate. Required for HTTPS to work |
| SSL_KEY | Path to SSL private key. Required for HTTPS to work |
| LOG_MODE | Log Mode. values: DEFAULT, SILENT, DEBUG |
| API_DOCS | Set it to `YES` to generate Swagger api documentation in the `/api-docs/` path. |

In order to configure the source, set the following variables:

| Variable Name | Description |
|---|---|
| SOURCE_PUB_KEY | External source public key |
| SOURCE_SECRET_KEY | External source secret key |
| MONGO_URI | Mongo database connection URI |
| MONGO_COLLECTION | Name of the Mongo DB collection to connect |
| SOURCE_FIELDS | List of fields, split by commas. Example: `sepalLength,sepalWidth,petalLength,petalWidth,species` |
| SOURCE_FIELDS_TYPES | For each field, its type. Types are: `nominal`, `numeric`, `logic`, `date` and `text`.
