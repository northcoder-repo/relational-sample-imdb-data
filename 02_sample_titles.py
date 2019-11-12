
import io
from shutil import copyfile

# for number formatting on console:
import locale
locale.setlocale(locale.LC_ALL, 'en_US')

# -----------------------------------------------------
# -----------------------------------------------------
# This is the approx. number of title records to grab:
#
#
sample_size = 2000
#
#
# Other data will be sampled based on these title IDs.
# The sample size is approximate because nobody likes
# nice round numbers. Also, bear in mind that an episode
# belongs to a series - which means 2 title records are
# needed. Otherwise you will have episode names (not
# often very useful), but no series name.
#
# Therefore...
#
#
series_thresh=100
#
#
# ...the above threshold is used to ensure you have at
# least some series titles for sampled episode titles.
# -----------------------------------------------------
# -----------------------------------------------------

print('')
print('Starting...')

# --------------------------------------------------------------------------

def line_count(fname):
    with io.open(fname, mode='r', encoding='utf-8') as f:
        for i, l in enumerate(f):
            pass
    return i

# --------------------------------------------------------------------------

title_records = line_count('csv/title.csv')

print('')
print(f"Total source titles:            {title_records:13n}")
print('')

sample_freq = round(title_records / (sample_size), 0)

# --------------------------------------------------------------------------
# titles

title_ids = set()
episodes = set() # will be used later to get some series records
i = 1
with io.open('csv/title.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            i += 1
            if i % sample_freq == 0:
                out_f.write(line)
                title_ids.add(line.split(',')[0])
                if line.split(',')[1] == '5': # content type for TV episodes
                    episodes.add(line.split(',')[0])

print(f"Sampled titles:                 {len(title_ids):13n}")

# --------------------------------------------------------------------------
# talent titles


# For talent names, we look in 2 places:
#   1) the talent titles CSV file (here)
#   2) the title principals CSV file (next)

talent_ids = set()

i = 0
with io.open('csv/talent_title.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/talent_title.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            talent_id = fields[0]
            title_id = fields[1].strip()
            if title_id in title_ids:
                i += 1
                talent_ids.add(talent_id)
                out_f.write(line)

print(f"Sampled talent titles:          {i:13n}")

# --------------------------------------------------------------------------
# title principals
# SEE ALSO talent titles above.

i = 0
with io.open('csv/title_principal.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title_principal.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            title_id = fields[0]
            talent_id = fields[1].strip()
            if title_id in title_ids:
                talent_ids.add(talent_id)
                i += 1
                out_f.write(line)

print(f"Sampled title principals:       {i:13n}")

# --------------------------------------------------------------------------
# talent

with io.open('csv/talent.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/talent.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            talent_id = fields[0]
            if talent_id in talent_ids:
                out_f.write(line)

print(f"Sampled talent:                 {len(talent_ids):13n}")

# --------------------------------------------------------------------------
# talent roles

i = 0
with io.open('csv/talent_role.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/talent_role.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            talent_id = fields[0]
            if talent_id in talent_ids:
                i += 1
                out_f.write(line)

print(f"Sampled talent roles:           {i:13n}")

# --------------------------------------------------------------------------
# title akas

i = 0
with io.open('csv/title_aka.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title_aka.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            title_id = fields[0]
            if title_id in title_ids:
                i += 1
                out_f.write(line)

print(f"Sampled title akas:             {i:13n}")

# --------------------------------------------------------------------------
# title aka title types

i = 0
with io.open('csv/title_aka_title_type.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title_aka_title_type.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            title_id = fields[0]
            if title_id in title_ids:
                i += 1
                out_f.write(line)

print(f"Sampled title aka title types:  {i:13n}")

# --------------------------------------------------------------------------
# title genres

i = 0
with io.open('csv/title_genre.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title_genre.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            title_id = fields[0]
            if title_id in title_ids:
                i += 1
                out_f.write(line)

print(f"Sampled title genres:           {i:13n}")

# --------------------------------------------------------------------------
# title series - here we may need to capture some more title records,
# to account for cases where we have episode records, but not the
# parent series records.  But only up to 'series_thresh' limit.

missing_series = set()

i = 0
with io.open('csv/title_episode.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title_episode.csv', mode='w', encoding='utf-8') as out_f:
        out_f.write(next(in_f))
        for line in in_f:
            fields = line.split(',')
            title_id = fields[0]
            parent_title_id = fields[1]
            if title_id in title_ids and parent_title_id in title_ids:
                # we already have the parent (series) for the child (episode):
                i += 1
                out_f.write(line)
            elif title_id in episodes and i < series_thresh:
                # we do not have the parent (series) for this episode:
                missing_series.add(parent_title_id)
                i += 1
                out_f.write(line)

print(f"Sampled title episodes:         {i:13n}")

# -------------------------------------------------------

# here we grab the extra series titles we need for the titles file:
i = 0
with io.open('csv/title.csv', mode='r', encoding='utf-8') as in_f:
    with io.open('csv/sampled/title.csv', mode='a', encoding='utf-8') as out_f:
        next(in_f)
        for line in in_f:
            fields = line.split(',')
            if fields[0] in missing_series:
                i += 1
                out_f.write(line)

print(f"Sampled extra series titles:    {i:13n}")

# --------------------------------------------------------------------------
# other small ref data files

print('')
print('Copying reference data files')
print('')

files = ['region', 'role', 'language', 'genre',
        'category', 'content_type', 'title_type']

for file in files:
    copyfile('csv/' + file + '.csv', 'csv/sampled/' + file + '.csv')

print('Finished.')
print('')
