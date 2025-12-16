# ReplayMod Center Archive

A tool made to archive the entire ReplayMod Center database of replays with the intention of preserving the history of Minecraft servers.


## Features
- Attempts to download all replays from the ReplayMod Center.
- Keeps track of downloaded replays and its metadata.
- Resumable downloads in case of interruptions.

## To be added
- Allows easy traversal of downloaded replays and their metadata.
- Search functionality for replays based on server, date, players, etc.
- User-friendly interface for browsing and managing the archive.


## How it works
The tool scrapes the ReplayMod Center website to gather a list of all available replays.  
To download a replay from the api, all it takes is an ID int, which luckily seems to be an incrementing integer starting from 0.

### Implementation details
- The tool uses a local sqlite database to keep track of downloaded replays and their metadata.
  - Table `replays` — stores an ID (autoincrement in case of a re-download giving a different replay ID), the ReplayMod Center replay ID, sha256 hash of the replay file, filesize in bytes, and download timestamp.  
    There doesn't seem to be anything else returned from the download endpoint. Anything we derive from the replay file itself can be extracted later, so we keep this table minimal.  
    If a replay doesn't exist, we store a record with a NULL hash to avoid re-attempting the download (at least too often).


## Why?

With the majority of Minecraft servers being shut down over time, the history of these servers is at risk of being lost forever.  
As a long time player, I have fond memories of various servers I've played on, and the replays recorded using ReplayMod are a significant method of preserving it, as they give the **full feel** of the gameplay, including world state and player skins.  
In the event of the ReplayMod Center going offline or losing data, these replays could be lost forever, too.


## Contact 

If you have any questions or ideas for this project, feel free to msg me on [my Discord](https://marcloud.net/discord)! ^_^

