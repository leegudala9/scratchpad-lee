#Snorkel Entity Tagging Experiment

First things first:

1) Run the setup.sh
      a) Just hit enter when it prompts for a git branch details. Basically, it is asking you to select the branch for Entity Taggining repo and formatter repo.
      b) You can also give a specific branch for this entity tagger and formatter repos.
2) Now place the rids list in a plain text file(i.e not a csv). Remember to keep the file name exactly as doc name to avoid failures.
3) This file has to stay inside rids_to_process folder.
3) Run the sherlock_snorkel.sh file with one argument which is "rids_to_process"

