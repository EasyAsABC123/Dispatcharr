# XMLTV gzip rejection and recovery on 1b490f5d

The pinned Original and the ordinary Linux AMD64 Rust package passed three signed refresh requests each against a disposable XMLTV origin. Both installed the same initial gzip archive. Each origin transferred 26,444 compressed bytes and each backend stored the same 6,750,261 decoded XML bytes and one programme.

At the same retry URL, the origin served a valid gzip archive containing incomplete XML. Both applications accepted the refresh with 202, recorded the file error and one audit event, and cleared the offset-index marker. The old cache path, device, inode, mtime, byte count, and digest stayed intact. The programme rows and IDs stayed intact. The root refresh tasks both reached Celery SUCCESS and produced no children.

The next signed refresh at that URL used a distinct task. Both origin responses contained the same 185 compressed bytes and 265 decoded bytes. Each backend promoted XML with the origin's exact decoded digest, replaced the old programme, rebuilt the offset index, and cleared all temporary download and extraction paths. Each refresh produced a successful programme-index task and a successful Series evaluation task tied to the new parent. The earlier source error remained the sole new audit event.

The paired owner and both backend runners exited 0. The exact two Original and three Rust container identities and process generations retired. Both projects had empty container, network, and volume inventories. The earlier attempt exited 1 before Compose because its fixture snapshot omitted the coordinator's `functional_lease.py` sibling. The recovery receipt verified that the UID probe retired, the process group and outputs settled, and no Compose resources existed. The successful attempt restored the exact pinned sibling.

The test used synthetic guide data and a local fixture origin. Full remote XMLTV coverage, performance, and formal M6 acceptance remain open. Original Dispatcharr, Django, Requests, Celery, and dependency authors retain full credit and licenses. [Sanitized evidence](../scripts/rust-integration/m6-1b490f5d-amd64-xmltv-gzip-retry.json)
