
function print_separator {
	echo "****************************************"
}

print_separator
$1/build/script_loader $1/data/scripts/snb.schema
print_separator
$1/build/script_loader $1/data/scripts/person.load
print_separator
$1/build/script_loader $1/data/scripts/place.load
print_separator
$1/build/script_loader $1/data/scripts/organisation.load
print_separator
$1/build/script_loader $1/data/scripts/tagclass.load
print_separator
$1/build/script_loader $1/data/scripts/tag.load
print_separator
$1/build/script_loader $1/data/scripts/forum.load
print_separator
$1/build/script_loader $1/data/scripts/post.load
print_separator
$1/build/script_loader $1/data/scripts/comment.load
print_separator
$1/build/script_loader $1/data/scripts/knows.load
print_separator
$1/build/script_loader $1/data/scripts/isPartOf.load
print_separator
$1/build/script_loader $1/data/scripts/isLocatedIn.load
print_separator
$1/build/script_loader $1/data/scripts/studyAt.load
print_separator
$1/build/script_loader $1/data/scripts/workAt.load
print_separator
$1/build/script_loader $1/data/scripts/hasType.load
print_separator
$1/build/script_loader $1/data/scripts/isSubclassOf.load
print_separator
$1/build/script_loader $1/data/scripts/hasInterest.load
print_separator
$1/build/script_loader $1/data/scripts/hasMember.load
print_separator
$1/build/script_loader $1/data/scripts/hasMemberWithPosts.load
print_separator
$1/build/script_loader $1/data/scripts/hasModerator.load
print_separator
$1/build/script_loader $1/data/scripts/hasTag.load
print_separator
$1/build/script_loader $1/data/scripts/likes.load
print_separator
$1/build/script_loader $1/data/scripts/postHasCreator.load
print_separator
$1/build/script_loader $1/data/scripts/postHasTag.load
print_separator
$1/build/script_loader $1/data/scripts/commentHasTag.load
print_separator
$1/build/script_loader $1/data/scripts/containerOf.load
print_separator
$1/build/script_loader $1/data/scripts/commentHasCreator.load
print_separator
$1/build/script_loader $1/data/scripts/replyOf.load
print_separator
$1/build/script_loader $1/data/scripts/emailaddress.load
print_separator
$1/build/script_loader $1/data/scripts/email.load
print_separator
$1/build/script_loader $1/data/scripts/language.load
print_separator
$1/build/script_loader $1/data/scripts/speaks.load
print_separator
