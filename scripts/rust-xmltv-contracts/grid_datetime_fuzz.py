"""Actual Source Django/Python ISO grammar fuzz; no replacement parser."""
import hashlib,inspect
from pathlib import Path

def vectors():
    values=set(['','bad','2026-11-09','20261109','2026-W46-1','2026W461'])
    for year in [0,1,4,100,1900,2000,2024,2026,9999]:
        for month in [0,1,2,4,11,12,13]:
            for day in [0,1,28,29,30,31,32]:
                values.update([f'{year:04}-{month:02}-{day:02}',f'{year:04}{month:02}{day:02}'])
    for year in [0,1,2004,2020,2021,2026,9999]:
        for week in [0,1,52,53,54]:
            for day in range(9):values.update([f'{year:04}-W{week:02}-{day}',f'{year:04}W{week:02}{day}'])
    for date in ['2026-11-09','20261109','2026-W46-1','2026W461','00010101','99991231']:
        for separator in ['T','t',' ','\n','\t','X','💡','\x00','\u2028','\u0301','12']:
            for clock in ['12','1200','12:00','120000','12:00:00','1:02','01:2','24:00','2400','23:60','2360','23:59:60','235960','12.5','12,123456789','12:00.5','12:00:00.123456789','01:02:03.']:
                values.add(date+separator+clock)
    for date in ['2026-11-09T12:34:56','20261109T123456','2026-11-09T1:2','2026-11-09']:
        for zone in ['Z','z','+0','+00','+0000','+00:00','-00:00','+23:59','+24:00','+25:00','-25:30','+23:60','+2400','+00:00:60','+00:00:59.123456789','+00:00.999','+05.5','+05:30,25','+05:30:22.123456789','+05:30:61','+99:99',' UTC','[UTC]']:
            values.add(date+zone)
    for raw in ['2026-11-09','20261109','2026-11-09T12:34','2026-1-2T1:2','2026-02-30','20260230','2026-11-09T24:00','2026-11-09T2400']:
        for left,right in [('', ' '),(' ',''),('', '\n'),('', '\r\n'),('', '\x1c'),('', '\u0085'),('', '\u2007')]:values.add(left+raw+right)
        for base in [0x660,0x6F0,0xFF10]:values.add(''.join(chr(base+int(ch))if ch.isascii()and ch.isdigit()else ch for ch in raw))
    # Exercise actual C date.fromisoformat length/prefix behavior separately
    # from datetime grammar and Django's two regex fallbacks.
    import itertools
    suffixes=set(['','0012','1200','1234','12345','123456','1234567','12345678','010203','T01','T012','T0123','T01234','T012345','T0123456','T12:34','0:12','1:23','12:345'])
    suffixes.update(['0','1','2','7','9','A','T','Z','-',' ','\t','\r','\n','\x00','é','\u0085','💡'])
    suffixes.update(''.join(parts)for parts in itertools.product(['0','1','9','X','T','-',' ','\r','\n','\x00','é','\u0085'],repeat=2))
    suffixes.update(''.join(parts)for parts in itertools.product('012359',repeat=3))
    for base in ['20261109','20260230','00010101','00000101','99991231','2026W46','2026W461','2020W537','2026-W46','2026-W46-1','2026-11-09']:
        values.update(base+suffix for suffix in suffixes)
    return sorted(values)

def capture(start=0,count=64):
    from apps.epg import api_grid
    from django.utils import dateparse
    assert 0<=start<20000 and 1<=count<=64
    values=vectors();cases=[]
    for raw in values[start:start+count]:
        row={'raw':raw,'name':'start'}
        try:row['value']=api_grid._parse_grid_datetime(raw,'start').isoformat()
        except Exception as error:row.update(error=type(error).__name__,message=str(error))
        cases.append(row)
    return {'scope':'Actual original _parse_grid_datetime via Django/Python; pure native comparison separate','total':len(values),'cases':cases,'source_sha256':hashlib.sha256(Path(api_grid.__file__).read_bytes()).hexdigest(),'django_dateparse_sha256':hashlib.sha256(Path(inspect.getfile(dateparse)).read_bytes()).hexdigest()}
