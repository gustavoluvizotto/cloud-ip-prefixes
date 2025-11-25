# cloud-ip-prefixes

## Requirements

You need ``podman`` installed in your machine.

## Usage

Please first build the docker image:

```
./build.sh
```

Then you can run the script to collect the Cloud prefixes:

```
./run.sh
```

## Note

The script will fail to upload the data. That's because you need s3 object store access. But you don't need this access to download the Cloud prefixes.

## License

MIT License

