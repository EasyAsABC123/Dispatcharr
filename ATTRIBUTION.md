# Original project attribution

Full credit for Dispatcharr's original application, design, features, user
interface, implementation, documentation, and test suite belongs to the original
Dispatcharr authors and contributors.

Dispatcharr began as a project by [OkinawaBoss](https://github.com/OkinawaBoss),
with contributions from [dekzter](https://github.com/dekzter),
[SergeantPanda](https://github.com/SergeantPanda),
[Seth](https://github.com/sethwv), and the entire
[upstream contributor community](https://github.com/Dispatcharr/Dispatcharr/graphs/contributors).
The upstream project's own acknowledgments remain in [README.md](README.md).
This list highlights the authors credited there; it is not an exhaustive list
of everyone who contributed.

* Original project: [Dispatcharr/Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).
* Compatibility baseline: [upstream revision 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430](https://github.com/Dispatcharr/Dispatcharr/tree/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430).
* Original authorship and contribution history: retained in this repository's Git history.
* License: [GNU Affero General Public License version 3](LICENSE), retained;
  the Rust crates declare `AGPL-3.0-only`, matching the original project's package metadata.

This fork contains an in-progress derivative Rust backend port and verification
tooling. It does not claim authorship of the original project or imply upstream
endorsement. Rust ports and compatibility tests use the original implementation
as their behavioral reference. Existing copyright, license, attribution, and
third-party notices remain in place.

Performance reports describe measured porting experiments. Any component gains
do not diminish the original authors' work and do not establish whole-application
gains unless the documented full-service comparisons pass.

## Scheduler framework notices

The scheduler compatibility work follows the pinned Original installation of Celery and django-celery-beat. The following original notices and author list are preserved verbatim; application credit and AGPL licensing above remain unchanged.

### celery 5.6.3 — LICENSE

```text
Copyright (c) 2017-2026 Asif Saif Uddin, core team & contributors. All rights reserved.
Copyright (c) 2015-2016 Ask Solem & contributors. All rights reserved.
Copyright (c) 2012-2014 GoPivotal, Inc.  All rights reserved.
Copyright (c) 2009, 2010, 2011, 2012 Ask Solem, and individual contributors. All rights reserved.

Celery is licensed under The BSD License (3 Clause, also known as
the new BSD license).  The license is an OSI approved Open Source
license and is GPL-compatible(1).

The license text can also be found here:
http://www.opensource.org/licenses/BSD-3-Clause

License
=======

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Ask Solem, nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Ask Solem OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

Documentation License
=====================

The documentation portion of Celery (the rendered contents of the
"docs" directory of a software distribution or checkout) is supplied
under the "Creative Commons Attribution-ShareAlike 4.0
International" (CC BY-SA 4.0) License as described by
https://creativecommons.org/licenses/by-sa/4.0/

Footnotes
=========
(1) A GPL-compatible license makes it possible to
    combine Celery with other software that is released
    under the GPL, it does not mean that we're distributing
    Celery under the GPL license.  The BSD license, unlike the GPL,
    let you distribute a modified version without making your
    changes open source.
```

### django-celery-beat 2.9.0 — AUTHORS

```text
=========
 AUTHORS
=========
:order: sorted

Aaron Ross <aaron@wawd.com>
Adam Endicott
Alex Stapleton <alex.stapleton@artfinder.com>
Alexandr Artemyev <mogost@gmail.com>
Alvaro Vega <avega@tid.es>
Andrew Frankel
Andrew Watts <andrewwatts@gmail.com>
Andrii Kostenko <andrey@kostenko.name>
Anton Novosyolov <anton.novosyolov@gmail.com>
Ask Solem <ask@celeryproject.org>
Asif Saif Uddin <auvipy@gmail.com>
Augusto Becciu <augusto@becciu.org>
Ben Firshman <ben@firshman.co.uk>
Brad Jasper <bjasper@gmail.com>
Brett Gibson <brett@swiftserve.com>
Brian Rosner <brosner@gmail.com>
Charlie DeTar <cfd@media.mit.edu>
Christopher Grebs <cg@webshox.org>
Dan LaMotte <lamotte85@gmail.com>
Darjus Loktevic <darjus@amazon.com>
David Fischer <david.fischer.ch@gmail.com>
David Ziegler <david.ziegler@gmail.com>
Diego Andres Sanabria Martin <diegueus9@gmail.com>
Dmitriy Krasilnikov <krasilnikov.d.o@gmail.com>
Donald Stufft <donald.stufft@gmail.com>
Eldon Stegall
Eugene Nagornyi <ideviantik@gmail.com>
Felix Berger <bflat1@gmx.net
Gabe Jackson <gabejackson@cxg.ch>
Glenn Washburn <M8R-hkaf6e@mailinator.com>
Gnrhxni <gnrhxni@outlook.com>
Greg Taylor <gtaylor@duointeractive.com>
Grégoire Cachet <gregoire@audacy.fr>
Hari <haridara@gmail.com>
Idan Zalzberg <idanzalz@gmail.com>
Ionel Maries Cristian <ionel.mc@gmail.com>
Jaeyoung Heo <jay.jaeyoung@gmail.com>
Jannis Leidel <jannis@leidel.info>
Jason Baker <amnorvend@gmail.com>
Jay States <jstates@based.ca>
Jeff Balogh <me@jeffbalogh.org>
Jeff Fischer <jeffrey.fischer@gmail.com>
Jeffrey Hu <zhiwehu@gmail.com>
Jens Alm <jens.alm@mac.com>
Jerzy Kozera <jerzy.kozera@gmail.com>
Jesper Noehr <jesper@noehr.org>
Jimmy Bradshaw <james.g.bradshaw@gmail.com>
Joey Wilhelm <tarkatronic@gmail.com>
John Andrews <johna@stjit011.(none)>
John Watson <johnw@mahalo.com>
Jonas Haag <jonas@lophus.org>
Jonatan Heyman <jonatan@heyman.info>
Josh Drake <m0nikr@is-0338.(none)>
José Moreira <zemanel@zemanel.eu>
Jude Nagurney <jude@pwan.org>
Justin Quick <justquick@gmail.com>
Keith Perkins <keith@tasteoftheworld.us>
Kirill Panshin <kipanshi@gmail.com>
Mark Hellewell <mark.hellewell@gmail.com>
Mark Heppner <mheppner@users.noreply.github.com>
Mark Lavin <markdlavin@gmail.com>
Mark Stover <stovenator@gmail.com>
Maxim Bodyansky <bodyansky@gmail.com>
Michael Elsdoerfer <michael@elsdoerfer.com>
Michael van Tellingen <m.vantellingen@lukkien.com>
Mikhail Korobov <kmike84@gmail.com>
Olivier Tabone <olivier.tabone@gmail.com>
Patrick Altman <paltman@gmail.com>
Piotr Bulinski <piotr@bulinski.pl>
Piotr Sikora <piotr.sikora@frickle.com>
Reza Lotun <rlotun@gmail.com>
Rockallite Wulf <rockallite.wulf@gmail.com>
Roger Barnes <roger@mindsocket.com.au>
Roman Imankulov <roman@netangels.ru>
Rune Halvorsen <runefh@gmail.com>
Sam Cooke <sam@mixcloud.com>
Scott Rubin <srubin@broadway.com>
Sean Creeley <sean.creeley@gmail.com>
Serj Zavadsky <fevral13@gmail.com>
Simon Charette <charette.s@gmail.com>
Spencer Ellinor <spencer.ellinor@gmail.com>
Theo Spears <github@theos.me.uk>
Timo Sugliani
Vincent Driessen <vincent@datafox.nl>
Vitaly Babiy <vbabiy86@gmail.com>
Vladislav Poluhin <nuklea@gmail.com>
Weipin Xia <weipin@me.com>
Wes Turner <wes.turner@gmail.com>
Wes Winham <winhamwr@gmail.com>
Williams Mendez <wmendez27@gmail.com>
WoLpH <Rick@Fawo.nl>
dongweiming <ciici123@hotmail.com>
```

### django-celery-beat 2.9.0 — LICENSE

```text
Copyright (c) 2015-2016 Ask Solem.  All Rights Reserved.
Copyright (c) 2012-2014 GoPivotal, Inc.  All Rights Reserved.
Copyright (c) 2009-2012 Ask Solem.  All Rights Reserved.

django-celery-beat is licensed under The BSD License (3 Clause, also known as
the new BSD license).  The license is an OSI approved Open Source
license and is GPL-compatible(1).

The license text can also be found here:
http://www.opensource.org/licenses/BSD-3-Clause

License
=======

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Ask Solem nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Ask Solem OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

Documentation License
=====================

The documentation portion of django-celery-beat (the rendered contents of the
"docs" directory of a software distribution or checkout) is supplied
under the "Creative Commons Attribution-ShareAlike 4.0
International" (CC BY-SA 4.0) License as described by
http://creativecommons.org/licenses/by-sa/4.0/

Footnotes
=========
(1) A GPL-compatible license makes it possible to
    combine django-celery-beat with other software that is released
    under the GPL, it does not mean that we're distributing
    django-celery-beat under the GPL license.  The BSD license, unlike the GPL,
    let you distribute a modified version without making your
    changes open source.
```

### Kombu 5.6.2 — LICENSE

```text
Copyright (c) 2015-2016 Ask Solem & contributors.  All rights reserved.
Copyright (c) 2012-2014 GoPivotal Inc & contributors.  All rights reserved.
Copyright (c) 2009-2012, Ask Solem & contributors.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Ask Solem nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Ask Solem OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```
