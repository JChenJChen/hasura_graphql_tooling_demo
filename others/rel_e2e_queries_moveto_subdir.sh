mkdir subdir
for i in group_1* group_2* group_3* group_4*; do
  git mv $i ../subdir/queries
done
