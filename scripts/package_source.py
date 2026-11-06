"""Build a reviewed public-source ZIP from Git-visible files, without Git history."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = {'.gitignore', '.gitattributes', 'README.md', 'CONTRIBUTING.md', 'SECURITY.md',
              'LICENSE', 'pyproject.toml', 'ingest.example.toml', 'requirements-build.lock',
              'requirements-dev.lock', 'requirements-runtime.lock'}
DIRECTORIES = {'db_ingest', 'docs', 'examples', 'scripts', 'tests', '.github'}
SECRET_PATTERNS = [rb'github_pat_[A-Za-z0-9_]{30,}', rb'gh[pousr]_[A-Za-z0-9]{30,}',
                   rb'AKIA[0-9A-Z]{16}', rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----']


def validate_file(root, relative, known_secrets=()):
    parts=Path(relative).parts
    if not parts or (len(parts)==1 and relative not in ROOT_FILES) or (len(parts)>1 and parts[0] not in DIRECTORIES):
        raise ValueError('Not in public source allowlist: '+relative)
    name=Path(relative).name.lower()
    if any(part in ('.git','.integration','.private-archive','__pycache__') or part.startswith('.venv') for part in parts):
        raise ValueError('Private/generated directory: '+relative)
    if any(word in name for word in ('credentials','secrets','handoff','architecture_prompt','agentic_loop')) or name.startswith('.env'):
        raise ValueError('Private/internal filename: '+relative)
    if Path(relative).suffix.lower() in ('.pyc','.pfx','.p12','.pem','.key','.exe','.msi','.whl','.zip','.log'):
        raise ValueError('Generated or sensitive file type: '+relative)
    path=root/relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('Source link escapes repository or is unsupported: '+relative)
    if path.stat().st_size>20*1024*1024:raise ValueError('Oversized source file: '+relative)
    data=path.read_bytes()
    if any(re.search(pattern,data) for pattern in SECRET_PATTERNS) or any(secret and secret in data for secret in known_secrets):
        raise ValueError('Possible credential in '+relative+'; value withheld')
    return data


def validate_links(root, files):
    included=set(files)
    for relative in files:
        if not relative.endswith('.md'):continue
        for target in re.findall(r'\]\(([^)]+)\)',(root/relative).read_text(encoding='utf-8')):
            target=unquote(target.split('#',1)[0])
            if not target or re.match(r'[A-Za-z][A-Za-z0-9+.-]*:',target):continue
            path=(root/relative).parent/target
            resolved=path.resolve()
            if not resolved.is_relative_to(root.resolve()) or resolved.relative_to(root.resolve()).as_posix() not in included:
                raise ValueError('Unpackaged documentation link in '+relative+': '+target)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():parser.error('Output exists; choose a new ZIP filename')
    tracked=subprocess.check_output(['git','-C',str(ROOT),'ls-files','--cached','--others','--exclude-standard','-z'])
    files=sorted(set(p.decode('utf-8').replace('\\','/') for p in tracked.split(b'\0') if p))
    secrets=[v.encode() for k,v in os.environ.items() if len(v)>=12 and any(word in k.upper() for word in ('TOKEN','PASSWORD','SECRET'))]
    # Validate everything before creating the archive. Never print matched secret values.
    for relative in files:validate_file(ROOT,relative,secrets)
    validate_links(ROOT,files)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(args.output,'x',zipfile.ZIP_DEFLATED) as archive:
        for relative in files:archive.writestr('db-ingest/'+relative,validate_file(ROOT,relative,secrets))
    digest=hashlib.sha256(args.output.read_bytes()).hexdigest()
    Path(str(args.output)+'.sha256').write_text(digest+'  '+args.output.name+'\n',encoding='utf-8')
    print(f'Public source: {len(files)} files; SHA256 {digest}')


if __name__=='__main__':main()
