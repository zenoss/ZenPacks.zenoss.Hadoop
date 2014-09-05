set -x

zenpack --remove ZenPacks.zenoss.Hadoop
zenpack --link --install .

zopectl restart
zenhub restart

set +x
