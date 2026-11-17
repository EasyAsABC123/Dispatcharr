"""Exact manual CUT decoded-content/time comparison, not container-byte equality.
Original Dispatcharr/Comskip/FFmpeg authors retain application and codec credit.
"""
import hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent

def compare_manual_cut(framehash_path, *, decoder_image, ffmpeg_sha256, decoder_argv, input_path):
    """Caller supplies the actual settled command and owned input binding.
    Only its one -i pathname may differ. No frame/sample/time/header is masked.
    """
    authority=json.loads((HERE/'manual-cut-content.json').read_text())
    assert decoder_image==authority['decoder_image']
    assert ffmpeg_sha256==authority['ffmpeg_sha256']
    actual=list(decoder_argv);expected=authority['decoder_argv']
    assert actual.count(input_path)==1 and actual[actual.index(input_path)-1]=='-i'
    actual[actual.index(input_path)]=expected[expected.index('-i')+1]
    assert actual==expected,'Exact all-stream decoder options required'
    digest=hashlib.sha256();size=0;counts={0:0,1:0};header=[]
    with Path(framehash_path).open('rb')as source:
        for line in source:
            size+=len(line);assert size<=8388608
            digest.update(line)
            text=line.decode('ascii')
            if text.startswith('#'):header.append(text.rstrip('\n'));continue
            if not text.strip():continue
            fields=[v.strip()for v in text.split(',')]
            assert len(fields)==6 and fields[0]in('0','1')
            assert all(v.lstrip('-').isdigit()for v in fields[1:5])
            assert len(fields[5])==64 and all(c in'0123456789abcdef'for c in fields[5])
            counts[int(fields[0])]+=1
    assert size==authority['framehash']['bytes']
    assert digest.hexdigest()==authority['framehash']['sha256'],'Full decoded content/time differs from Source'
    assert counts=={int(k):v for k,v in authority['framehash']['frame_counts'].items()}
    assert '#tb 0: 1/1000'in header and '#tb 1: 1/48000'in header
    assert '#codec_id 0: rawvideo'in header and '#codec_id 1: pcm_f32le'in header
    return {'passed':True,'full_decoded_all_stream_content_and_time_exact':True,'frames':counts,'framehash_sha256':digest.hexdigest(),'container_SHA_equivalence_claimed':False}
