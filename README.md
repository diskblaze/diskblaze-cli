# DiskBlaze CLI

Command-line client for [DiskBlaze](https://diskblaze.com) storage.

## Install

```bash
pip install git+https://github.com/diskblaze/diskblaze-cli
```

## Log in

```bash
diskblaze login              # prompts for an API token, or pass --token
diskblaze logout
```

The token is saved to `~/.config/diskblaze/credentials.json`. You can also set
`DISKBLAZE_TOKEN` instead of logging in.

## Commands

```bash
diskblaze whoami
diskblaze ls /private
diskblaze search invoice --path /private

diskblaze mkdir /private/backups
diskblaze mv /private/a.mkv /private/b.mkv
diskblaze rm /private/old.bin

diskblaze upload ./movie.mkv /private/movie.mkv     # ul
diskblaze upload ./folder /private/folder
diskblaze download /private/movie.mkv ./movie.mkv   # dl
diskblaze download /private/folder ./folder -r      # recursive
diskblaze download /private/folder ./folder.zip --zip
diskblaze url /private/movie.mkv --expires 604800   # signed link
```

Run `diskblaze <command> --help` for all options. `--workers` and
`--file-workers` tune upload/download concurrency.

## Python

```python
from diskblaze import DiskBlazeClient

client = DiskBlazeClient(token="db_...")   # or reads DISKBLAZE_TOKEN
client.upload_file("movie.mkv", "/private/movie.mkv")
client.download("/private/movie.mkv", "movie.mkv")
```

## License

[Apache 2.0](LICENSE)
