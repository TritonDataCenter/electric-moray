#!/usr/sbin/dtrace -s

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
