# F38 ARM64 formal S120 repeat on the existing VM

The same ordinary ARM64 binary at `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` passed the scoped S120 acceptance checks on an existing Docker VM. The repeat created fresh owned projects and volumes and used the original signed workload, limits, descriptor census and cleanup. It did not prime or reset the VM cache. The run and independent proof exited zero, with 25 active samples, 20 useful A/V captures, two signed guide terminals, full descriptor coverage, and an empty engine after cleanup.

| Working-set evidence | First formal attempt | Existing-VM repeat |
| --- | ---: | ---: |
| Whole-scope working bytes before | 398,086,144 B | 209,309,696 B |
| Whole-scope working bytes after | 488,333,312 B | 228,773,888 B |
| Whole-scope working growth | +90,247,168 B | +19,464,192 B |
| Candidate file charge before | 368,640 B | 11,366,400 B |
| Candidate file charge after | 71,991,296 B | 11,366,400 B |
| Candidate active-file growth | +70,664,192 B | 0 B |
| Unchanged scope growth limit | 67,108,864 B | 67,108,864 B |
| Independent acceptance | Failed | Passed |

The first run and repeat had materially different measured idle and cache states. The repeat supplies a scoped same-binary formal S120 predecessor. The earlier failure stands, and neither result supplies a general memory guarantee or comparative release performance. Any longer acceptance must independently replay this exact repeat and pass its original workload gates.

Repeat proof SHA-256 `535dad803692d16852a446953516d5974adda2488ee760975934a957c505420d`. Independent idle attribution SHA-256 `cc92de90dc347ff1d29b126feee64cf080c5067c44b745b6c793306c9893f96b`. Original idle attribution SHA-256 `6beb8eab19eec88de09bcf685554efc411c66cf727bc273d891ca86790e9a671`.

The original Dispatcharr authors and contributors retain full credit.
