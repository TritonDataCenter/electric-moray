#!/usr/sbin/dtrace -s
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

   /*#pragma D option quiet*/

electric-moray*:::putobject-start
{
	track[arg0] = timestamp;
}

electric-moray*:::putobject-done
/track[arg0] > 0/
{
	@latency["put"] = quantize((timestamp - track[arg0]) / 1000000);
	track[arg0] = 0;
}


electric-moray*:::getobject-start
{
	track[arg0] = timestamp;
}

electric-moray*:::getobject-done
/track[arg0] > 0/
{
	@latency["get"] = quantize((timestamp - track[arg0]) / 1000000);
	track[arg0] = 0;
}


electric-moray*:::delobject-start
{
	track[arg0] = timestamp;
}

electric-moray*:::delobject-done
/track[arg0] > 0/
{
	@latency["del"] = quantize((timestamp - track[arg0]) / 1000000);
	track[arg0] = 0;
}


electric-moray*:::findobjects-start
{
	track[arg0] = timestamp;
}

electric-moray*:::findobjects-done
/track[arg0] > 0 && arg1 >= 0/
{
	@latency["find"] = quantize((timestamp - track[arg0]) / 1000000);
}

electric-moray*:::findobjects-done
{
	track[arg0] = 0;
}
