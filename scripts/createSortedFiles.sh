#!/bin/bash


#for ((i=0;i<$1;i++)) 
#do
#    for ((j=0;j<$2;j++)) 
#    do
#        echo "SORTING post_${i}_${j}.csv"
#        (head -n 1 post_${i}_${j}.csv && tail -n +2 post_${i}_${j}.csv | sort -t "|" -k3 ) > post_${i}_${j}.csv.sorted
#        mv post_${i}_${j}.csv.sorted post_${i}_${j}.csv
#
#        echo "SORTING comment_${i}_${j}.csv"
#        (head -n 1 comment_${i}_${j}.csv && tail -n +2 comment_${i}_${j}.csv | sort -t "|" -k2 ) > comment_${i}_${j}.csv.sorted
#        mv comment_${i}_${j}.csv.sorted comment_${i}_${j}.csv
#
#        echo "SORTING person_${i}_${j}.csv"
#        (head -n 1 person_${i}_${j}.csv && tail -n +2 person_${i}_${j}.csv | sort -t "|" -k6 ) > person_${i}_${j}.csv.sorted
#        mv person_${i}_${j}.csv.sorted person_${i}_${j}.csv
#
#        echo "SORTING forum_${i}_${j}.csv"
#        (head -n 1 forum_${i}_${j}.csv && tail -n +2 forum_${i}_${j}.csv | sort -t "|" -k3 ) > forum_${i}_${j}.csv.sorted
#        mv forum_${i}_${j}.csv.sorted forum_${i}_${j}.csv
#
#        echo "SORTING person_likes_comment_${i}_${j}.csv"
#        (head -n 1 person_likes_comment_${i}_${j}.csv && tail -n +2 person_likes_comment_${i}_${j}.csv | sort -t "|" -k3 ) > person_likes_comment_${i}_${j}.csv.sorted
#        mv person_likes_comment_${i}_${j}.csv.sorted person_likes_comment_${i}_${j}.csv
#
#        echo "SORTING person_likes_post_${i}_${j}.csv"
#        (head -n 1 person_likes_post_${i}_${j}.csv && tail -n +2 person_likes_post_${i}_${j}.csv | sort -t "|" -k3 ) > person_likes_post_${i}_${j}.csv.sorted
#        mv person_likes_post_${i}_${j}.csv.sorted person_likes_post_${i}_${j}.csv
#
#        echo "SORTING person_knows_person_${i}_${j}.csv"
#        (head -n 1 person_knows_person_${i}_${j}.csv && tail -n +2 person_knows_person_${i}_${j}.csv | sort -t "|" -k3 ) > person_knows_person_${i}_${j}.csv.sorted
#        mv person_knows_person_${i}_${j}.csv.sorted person_knows_person_${i}_${j}.csv
#    done
#done

python sortCommentsByAuthor.py

for ((i=0;i<$1;i++)) 
do
    for ((j=0;j<$2;j++)) 
    do
        echo "SORTING comment_hasCreator_person_${i}_${j}.csv"
        (head -n 1 comment_hasCreator_person_${i}_${j}.csv && tail -n +2 comment_hasCreator_person_${i}_${j}.csv | sort -t "|" -k2 ) > comment_hasCreator_person_${i}_${j}.csv.sorted
        mv comment_hasCreator_person_${i}_${j}.csv.sorted comment_hasCreator_person_${i}_${j}.csv
        mv comment_${i}_${j}.csv.sorted comment_${i}_${j}.csv
    done
done
