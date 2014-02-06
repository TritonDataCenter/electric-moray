Background:

This assumes you already know what electric-moray(e-moray) does and how consistent hashing works. If not, you should ask someone.
The consistent hash ring is stored in a leveldb database on disk -- for all intends and purposes, we can just model it as a binary blob. Historically we pre generated a set of hash rings based on the deployment size of Manta, and packaged them up in the elctric-moray build image. When we deployed an e-moray, its setup scripts copied this blob onto disk from the build image. This is insufficient for a variety of reasons.

These rings are specific to joyent's particular installation of Manta -- in fact the shard names are baked into the ring. This means for any other deployments of Manta where the number and names of shard are different -- e.g. private Manta deployments, lab deployments, or deployments where the shard names are different, these rings will not work.

Since these rings are specific to Joyent's installation of Manta -- when changes need to be made to a ring after deployment via resharding, the new ring will have to be put back into the e-moray image. There is no segregation of rings between installations. Again, this doesn't work since rings need to be installation specific.

So what now?

The current plan is to remove ring topologies from the e-moray image all together. Instead we propose the following.

When deploying a new Manta, the manta-deployment tools generate a new topology from scratch based on the parameters of the installation.

The topology is stored as a tarball image in imgapi. Each tarball has a semver which is stored in sapi. e-moray on setup will fetch the ring version and download the tarball via imgapi.
