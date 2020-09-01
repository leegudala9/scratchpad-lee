#!/usr/bin/env bash

echo "which branch of Formatter: "
read formatter_git_branch
formatter_git_branch=${formatter_git_branch:-master}
echo "which branch of tagger: "
read tagger_git_branch
tagger_git_branch=${tagger_git_branch:-master}
echo "enter experiment id: "
read exp_id


token=$(aws ssm get-parameter --name /github/token | jq -r .Parameter.Value)
username=$(aws ssm get-parameter --name /github/username | jq -r .Parameter.Value)
echo $tagger_git_branch
echo $formatter_git_branch
echo $exp_id
echo $token

pip install -r snorkel_requirment.txt

rm -rf EntityTaggerService/
rm -rf entity_tagger/
git clone https://$username:$token@github.com/HeavyWater-Solutions/EntityTaggerService.git
cd EntityTaggerService
git checkout $tagger_git_branch
git pull
cd entity_tagger
pip3 install -r requirements.txt
cd ../
cp -r entity_tagger/ ../
cd ../
rm -rf EntityTaggerService/


rm -rf EntityFormatterService/
rm -rf entity_formatter/
git clone https://$username:$token@github.com/HeavyWater-Solutions/EntityFormatterService.git
cd EntityFormatterService
git checkout $formatter_git_branch
git pull
cd entity_formatter/
pip3 install usaddress -t .
pip3 install dateutil -t .
cd ../
cp -r entity_formatter/ ../
cd ../
rm -rf EntityFormatterService/
cp -r entity_formatter/* ./