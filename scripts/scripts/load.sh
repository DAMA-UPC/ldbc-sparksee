#!/bin/bash
set -e

function print_separator {
	echo "****************************************"
}

print_separator
time $1/build/script_loader $1/scripts/scripts/snb.schema
print_separator
time $1/build/script_loader $1/scripts/scripts/person.load
print_separator
time $1/build/script_loader $1/scripts/scripts/place.load
print_separator
time $1/build/script_loader $1/scripts/scripts/organisation.load
print_separator
time $1/build/script_loader $1/scripts/scripts/tagclass.load
print_separator
time $1/build/script_loader $1/scripts/scripts/tag.load
print_separator
time $1/build/script_loader $1/scripts/scripts/forum.load
print_separator
time $1/build/script_loader $1/scripts/scripts/post.load
print_separator
time $1/build/script_loader $1/scripts/scripts/comment.load
print_separator
time $1/build/script_loader $1/scripts/scripts/knows.load
print_separator
time $1/build/script_loader $1/scripts/scripts/isPartOf.load
print_separator
time $1/build/script_loader $1/scripts/scripts/isLocatedIn.load
print_separator
time $1/build/script_loader $1/scripts/scripts/studyAt.load
print_separator
time $1/build/script_loader $1/scripts/scripts/workAt.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasType.load
print_separator
time $1/build/script_loader $1/scripts/scripts/isSubclassOf.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasInterest.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasMember.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasMemberWithPosts.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasModerator.load
print_separator
time $1/build/script_loader $1/scripts/scripts/hasTag.load
print_separator
time $1/build/script_loader $1/scripts/scripts/likes.load
print_separator
time $1/build/script_loader $1/scripts/scripts/postHasCreator.load
print_separator
time $1/build/script_loader $1/scripts/scripts/postHasTag.load
print_separator
time $1/build/script_loader $1/scripts/scripts/commentHasTag.load
print_separator
time $1/build/script_loader $1/scripts/scripts/containerOf.load
print_separator
time $1/build/script_loader $1/scripts/scripts/commentHasCreator.load
print_separator
time $1/build/script_loader $1/scripts/scripts/replyOf.load
print_separator
time $1/build/script_loader $1/scripts/scripts/emailaddress.load
print_separator
time $1/build/script_loader $1/scripts/scripts/email.load
print_separator
time $1/build/script_loader $1/scripts/scripts/language.load
print_separator
time $1/build/script_loader $1/scripts/scripts/speaks.load
print_separator
