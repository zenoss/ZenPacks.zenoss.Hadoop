set -x

zenpack --remove ZenPacks.zenoss.Hadoop
cd ..
zenpack --link --install .

zopectl restart
zenhub restart

set +x
