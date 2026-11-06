# Windows paths and network sources



This release supports a Windows x64 importer. Use the offline bundle's standard Python

3.13 x64 environment. Install and keep the application and recovery work directory on

local storage. Network source files are supported subject to the access/locking conditions

below. The SQL server may be remote; this does not make the work directory remote.



## Supported spellings



TOML literal strings (single quotes) keep backslashes literal:



| Path | Meaning / support |

| --- | --- |

| `root = 'D:\Exports'` | Absolute local source directory. |

| `root = 'D:/Exports'` | Forward-slash spelling of the same Windows path. |

| `root = 'examples'` | Relative to the process working directory. Run from the extracted release folder. |

| `path = 'examples/quickstart.csv'` | Relative to the working directory, **not to source.root or the TOML file**. |

| `root = '\\server\share\Exports'` | UNC/SMB source. The current Windows account must have share and filesystem read/list access. |

| `root = 'Z:\Exports'` | Mapped drive source, only when that mapping exists in the account/session running the importer. Prefer UNC for scheduled jobs. |

| `root = '\\?\D:\Exports'` | Extended absolute local path. Use consistently for root and path; use backslashes. |

| `root = '\\?\UNC\server\share\Exports'` | Extended UNC source path. Use consistently for root and path. |

| `work_directory = 'D:\IngestWork'` | Local durable recovery/capture directory. Prefer a short absolute path. |

| `work_directory = '.local/work'` | Local work directory relative to the working directory; keep that location stable across runs. |



Spaces and Unicode are supported. Quote PowerShell path arguments as well as TOML values.

If using double-quoted TOML strings instead, backslashes must be escaped (`"D:\\Exports"`).

Omit/empty `source.path` to use root; otherwise select a file/subfolder inside root. Do not

use `path='orders.csv'` expecting it to be joined to an unrelated `root='D:\Exports'`.

`include`/`exclude` use relative glob patterns; prefer `/` separators. Matching is deliberately

case-sensitive, so include both `**/*.csv` and `**/*.CSV` if both suffixes occur.



`D:Exports` (drive-relative), `\Exports` (current-drive-rooted) and Windows device paths

such as `\\.\PhysicalDrive0` are rejected with CONFIG_PATH. Environment-variable strings,

`~`, URLs and wildcards are not expanded as filesystem paths; use actual paths and put

selection wildcards in include/exclude. Reserved names, invalid Windows filename characters

and overlong individual components remain subject to Windows/filesystem restrictions.

Symlinks and junctions are excluded by default. Network work locations reached through a

mapped drive or resolved link are refused too.



## UNC source example



Merge these sections into your otherwise configured TOML (do not duplicate section names):



```toml

[source]

root = '\\fileserver\exports\Orders'

# path omitted: select root

include = ['**/*.csv', '**/*.CSV']

recursive = true



[preservation]

work_directory = 'D:\IngestWork\orders'

```



SQL credentials do not authenticate Windows file shares. Have IT grant the Windows account

running the import read/list permissions and access to the share. Elevated sessions, services

and scheduled tasks may not see your interactive mapped drives. Never put share passwords in

TOML or command arguments. Test access using the same account that will run the importer.



During capture the Windows source handle denies ordinary write/delete opens. An existing

writer can make capture fail with CAPTURE_IO; close/pause the producer, inspect the retained

run, then use README recovery to resume that same run. Publish producer exports only when

complete. A successful open is not proof of a snapshot against every NAS/server failure.



**The work directory must be local.** UNC and mapped-network work directories fail with

WORK_DIRECTORY_NETWORK before journal creation. The SQLite recovery journal uses WAL;

[SQLite documents its network-filesystem restriction](https://www.sqlite.org/wal.html).

Do not store a shared journal on SMB, NAS, a cloud-sync folder, or a multi-host filesystem.

The application checks UNC/mapped/resolved network paths; it cannot identify every sync or

storage product. IT must approve the actual storage. Existing network-based work directories

are not silently moved or rewritten: preserve them and request a reviewed recovery migration.



## Long paths and bcp



Local and UNC source paths over 260 characters were exercised (up to 322 characters in this

rehearsal), including extended prefixes. This host has Windows LongPathsEnabled=1 and

standard Python 3.13.2. Have IT enable the Windows long-path policy where required and test

on the target machine; Explorer, backup tools and native clients may have different limits.

An extended prefix is not a universal workaround for those tools or filesystem component limits.



ODBC also passed with a local work directory over 300 characters and longer generated capture

paths. **bcp's generated transport input path must not exceed 255 characters.** Choose a short

local work directory for bcp, such as `D:\IngestWork\orders`, because the importer appends run,

attempt and chunk names. Long source paths still work with bcp when staging uses a short path.

Do not assume `\\?\` bypasses this bcp guard.



If an existing run stops with BCP_PATH_LIMIT, retain its configuration and work directory.

Make a recovery copy of that configuration, change only `[load] engine = 'odbc'`, and resume

its original run ID using that configuration; then reconcile it. This same-run recovery was

verified. Do not move the journal, rename the source or start a new intent to bypass failure.

For future bcp runs, configure a short work path before the first plan.



## Keep path identities stable



Keep the source root, relative paths, spelling convention, work location and execution

account stable across related runs. Do not switch between a drive letter, UNC alias and

extended prefix midway through a run. Filename ownership claims bind to the source root

and relative path; moving or remapping a source is a migration decision. Copying an active

venv/work folder is not the supported way to relocate an existing run.



## Verification scope



Real Windows SMB access through a loopback share was tested, including spaces/Unicode,

UNC/extended UNC, a temporary mapped drive, locking refusal and same-run recovery. ODBC

and native bcp verified exact SQL values, captures and single receipts after repeated resume.

Network work paths were refused. The long bcp work-path failure recovered with ODBC.



This does not certify a particular remote file server/NAS, cross-machine authentication,

physical network partitions, failover, DFS or power-loss behavior. Before deployment,

IT should run a representative import and recovery check against the actual share/account.

The release is Windows-only; no Linux importer support is claimed.


Abrupt SMB server/connection loss during capture and publication has now been tested with
both loaders; see [SMB outage recovery](smb-outage-recovery.md) for results and operator steps.

Source changes during capture: see [changed-export recovery](smb-outage-recovery.md).
Nonempty incomplete captures cannot be automatically recaptured under the same intent.
