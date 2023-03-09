 * https://dbtlearn.s3.amazonaws.com/hosts.csv
 * https://dbtlearn.s3.amazonaws.com/reviews.csv
 * https://dbtlearn.s3.amazonaws.com/listings.csv

```bash

for f in hosts reviews listings;
do
  echo curl https://dbtlearn.s3.amazonaws.com/$f.csv -o raw_$f.csv
done
```