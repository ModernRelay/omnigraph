| target | mode | writers | baseline w/s | baseline p99 ms | best variant | best w/s | best commits/s | best p99 ms | speed-up |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|
| s3 | same | 1 | 2.90 | 360 | opt-group-claim | 4.15 | 4.15 | 266 | 1.4× |
| s3 | same | 4 | 2.20 | 1910 | opt-group-pipe | 11.75 | 9.40 | 442 | 5.3× |
| s3 | same | 16 | 2.20 | 7504 | opt-group-claim | 42.60 | 5.40 | 565 | 19.4× |
| s3 | same | 64 | 2.40 | 22903 | opt-group-pipe | 72.25 | 7.75 | 2027 | 30.1× |
| s3 | separate | 1 | 1.55 | 661 | opt-group | 1.50 | 1.50 | 705 | 1.0× |
| s3 | separate | 4 | 4.45 | 1038 | opt-group | 6.85 | 6.85 | 610 | 1.5× |
| s3 | separate | 16 | 13.50 | 1413 | opt-group | 26.80 | 26.80 | 628 | 2.0× |
| s3 | separate | 64 | 32.80 | 4039 | opt-group | 60.74 | 60.74 | 4070 | 1.9× |
| local | same | 1 | 25.65 | 64 | opt-group | 24.25 | 24.25 | 67 | 0.9× |
| local | same | 4 | 21.00 | 498 | opt-group-pipe | 40.80 | 40.70 | 152 | 1.9× |
| local | same | 16 | 21.75 | 1106 | opt-group | 69.70 | 38.50 | 538 | 3.2× |
| local | same | 64 | 21.85 | 3923 | opt-group-claim | 69.00 | 18.75 | 2451 | 3.2× |
| local | separate | 1 | 23.70 | 66 | opt-group | 22.30 | 22.30 | 65 | 0.9× |
| local | separate | 4 | 61.55 | 106 | opt-group | 64.60 | 64.60 | 92 | 1.0× |
| local | separate | 16 | 122.55 | 496 | opt-group | 143.53 | 143.53 | 192 | 1.2× |
| local | separate | 64 | 180.84 | 525 | opt-group | 233.09 | 233.09 | 422 | 1.3× |
