#
# Takes imdb files from here...
#
#   https://datasets.imdbws.com/
#
# ...and re-formats them into 'normalized' csv files,
# suitable for loading into a relational schema. Not
# all of the source data is used - it's designed to
# just be a testing data set, based on something
# fairly familiar - movies and TV series.
#
# (By 'normalized', we mean in the relational sense.)
#
# ---------------------------------------------------------------
#
#   USAGE NOTES:
#
# This script assumes the files are already downloaded, and already
# placed in the same directory as this script. You should also create
# a sub-directory called "csv", and (optionally) a further sub-
# directory "csv/sampled" if you plan on using the related sampler
# script.
#
# This script assumes you are using Python 3.7 or later.
#
# ---------------------------------------------------------------
#
# The source files we need are:
#
#  - name.basics.tsv.gz
#  - title.akas.tsv.gz
#  - title.basics.tsv.gz
#  - title.principals.tsv.gz
#  - title.episode.tsv.gz
#
# The following are not currently handled by this script:
#
#  - title.crew.tsv.gz
#  - title.ratings.tsv.gz
#
# The following output files are generated:
#
#  - category.csv
#  - content_type.csv
#  - genre.csv
#  - language.csv
#  - region.csv
#  - role.csv
#  - talent.csv
#  - talent_role.csv
#  - talent_title.csv
#  - title.csv
#  - title_aka.csv
#  - title_aka_title_type.csv
#  = title_episode.csv
#  - title_genre.csv
#  - title_principal.csv
#  - title_type.csv
#
# Each output file is suitable to be loaded into a DB table. An example
# of doing so with the H2 database is available on GitHub.
#

import gzip
import io
import shutil
import csv
import time
import re
from datetime import datetime
# for number formatting on console:
import locale
locale.setlocale(locale.LC_ALL, 'en_US')

# -----------------------------------------------------------------------------------------------------------------------------

def unzip_files(files):
    for file in files:
        in_file_name = file + in_suffix
        out_file_name = file.replace('.', '_') + '.tsv'
        print("Unzipping " + in_file_name + " to " + out_file_name + ".")
        with gzip.open(in_file_name, 'rb') as f_in:
            with open(out_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

# -----------------------------------------------------------------------------------------------------------------------------

def line_count(fname):
    with io.open(fname, mode='r', encoding='utf-8') as f:
        for i, l in enumerate(f):
            pass
    print('Uncompressed ' + fname + f" line count: {i:13n}")
    return i

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

#
# Names are re-labeled as "talent". The 'name basics' imdb file contains 2 lists
# in each record - titles associated with a talent, and roles performed by the
# talent. So we re-normalize this data.  We have to create a role master and
# assign sequential IDs to it.
#
# Customized field names are used in various places to replace imdb values.
#
def normalize_name_basics():
    in_file_name = 'name_basics.tsv'
    roles_dict = {} # key is a name and value is an auto-assigned ID
    print("Processing data in " + in_file_name + ".")

    # set up the output files here
    tal = io.open('csv/talent.csv', mode='w', encoding='utf-8', newline='')
    tal_fields = ['talent_id', 'talent_name', 'birth_year', 'death_year']
    tal_writer = csv.DictWriter(tal, fieldnames=tal_fields)
    tal_writer.writeheader()

    tal_role = io.open('csv/talent_role.csv', mode='w', encoding='utf-8', newline='')
    tal_role_fields = ['talent_id', 'role_id', 'order']
    tal_role_writer = csv.DictWriter(tal_role, fieldnames=tal_role_fields)
    tal_role_writer.writeheader()

    tal_title = io.open('csv/talent_title.csv', mode='w', encoding='utf-8', newline='')
    tal_title_fields = ['talent_id', 'title_id']
    tal_title_writer = csv.DictWriter(tal_title, fieldnames=tal_title_fields)
    tal_title_writer.writeheader()

    role = io.open('csv/role.csv', mode='w', encoding='utf-8', newline='')
    role_fields = ['role_id', 'role_name']
    role_writer = csv.DictWriter(role, fieldnames=role_fields)
    role_writer.writeheader()

    count = line_count(in_file_name)

    with io.open(in_file_name, mode='r', encoding='utf-8') as f_in:
        i = 0
        next(f_in) # ignore 1st row's headings - we already have our new custom ones.
        for line in f_in:
            i += 1
            j = (i/count) * 100
            print(f' - processing file [%d%%]' % (j), end="\r")
            in_fields = line.strip().split('\t')
            tal_writer.writerow({'talent_id': in_fields[0], 'talent_name': in_fields[1], 'birth_year': in_fields[2], 'death_year': in_fields[3]})

            if in_fields[4]:
                roles_dict = collect_talent_roles(roles_dict, in_fields[4], in_fields[0], tal_role_writer)

            if in_fields[5]:
                collect_talent_titles(in_fields[0], in_fields[5], tal_title_writer)

    # Now we can write out our role master data to file:
    for key, val in roles_dict.items():
        # we reverse the dict keys/values here:
        clean_key = key.replace('_', ' ')
        role_writer.writerow({'role_id': val, 'role_name': clean_key})
    print(' - processed 100% of file.')

    tal.close()
    tal_role.close()
    tal_title.close()
    role.close()

# -------------------------------------------------------------------------

# As well as populating the talent-role file, we gather a master collection of roles, with
# unique IDs auto-assigned.
def collect_talent_roles(roles_dict, roles_string, talent_id, tal_role_writer):
    role_names = roles_string.split(',')
    index = 0
    for role_name in role_names:
        role_name = role_name.strip()
        index += 1
        if role_name and (role_name != '\\N'):
            if not (role_name in roles_dict):
                roles_dict[role_name] = len(roles_dict) + 1
            tal_role_writer.writerow({'talent_id': talent_id, 'role_id': roles_dict[role_name], 'order': index})
    return roles_dict

# -------------------------------------------------------------------------

# The talent-title file is populated here:
def collect_talent_titles(talent_id, titles_string, tal_title_writer):
    for title_id in titles_string.split(','):
        title_id = title_id.strip()
        if talent_id and title_id and (talent_id != '\\N') and (title_id != '\\N'):
            tal_title_writer.writerow({'talent_id': talent_id, 'title_id': title_id})

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

# We don't split out the "additional attributes", but we do split out the "title types". These 2 lists
# appear to use x02 as separators (not commas). Region and language will get their own decodes, but not
# here. Here we just capture the enumerations separately.
#
# Note that this input file has a compound primary key (title ID + order).
def normalize_title_akas():
    in_file_name = 'title_akas.tsv'
    title_types_dict = {} # key is a name and value is an auto-assigned ID
    regions = set()
    langs = set()
    print("Processing data in " + in_file_name + ".")

    # output files:
    ttl_aka = io.open('csv/title_aka.csv', mode='w', encoding='utf-8', newline='')
    ttl_aka_fields = ['title_id', 'order', 'aka_title', 'region', 'language', 'additional_attrs', 'is_original_title']
    ttl_aka_writer = csv.DictWriter(ttl_aka, fieldnames=ttl_aka_fields)
    ttl_aka_writer.writeheader()

    ttl_ttl_type = io.open('csv/title_aka_title_type.csv', mode='w', encoding='utf-8', newline='')
    ttl_ttl_type_fields = ['title_id', 'title_type_id', 'order']
    ttl_ttl_type_writer = csv.DictWriter(ttl_ttl_type, fieldnames=ttl_ttl_type_fields)
    ttl_ttl_type_writer.writeheader()

    ttl_type = io.open('csv/title_type.csv', mode='w', encoding='utf-8', newline='')
    ttl_type_fields = ['title_type_id', 'title_type_name']
    ttl_type_writer = csv.DictWriter(ttl_type, fieldnames=ttl_type_fields)
    ttl_type_writer.writeheader()

    region_f = io.open('csv/region.csv', mode='w', encoding='utf-8', newline='')
    region_fields = ['region_id', 'region_name']
    region_writer = csv.DictWriter(region_f, fieldnames=region_fields)
    region_writer.writeheader()

    language_f = io.open('csv/language.csv', mode='w', encoding='utf-8', newline='')
    language_fields = ['language_id', 'language_name']
    language_writer = csv.DictWriter(language_f, fieldnames=language_fields)
    language_writer.writeheader()

    count = line_count(in_file_name)

    with io.open(in_file_name, mode='r', encoding='utf-8') as f_in:
        i = 0
        next(f_in) # ignore 1st row's headings - we already have our new custom ones.
        for line in f_in:
            i += 1
            j = (i/count) * 100
            print(f' - processing file [%d%%]' % (j), end="\r")

            in_fields = line.strip().split('\t')

            if len(in_fields[2]) > 480:
                # discard over-long title AKA values:
                in_fields[2] = '\\N'

            if in_fields[6]:
                in_fields[6] = in_fields[6].replace('\x02', ', ')

            if in_fields[7]:
                in_fields[7] = in_fields[7].strip()

            if in_fields[2] and (in_fields[2] != '\\N'):
                ttl_aka_writer.writerow({'title_id': in_fields[0], 'order': in_fields[1], 'aka_title': in_fields[2], 'region': in_fields[3], 'language': in_fields[4], 'additional_attrs': in_fields[6], 'is_original_title': in_fields[7]})

            if in_fields[3] and (in_fields[3] != '\\N'):
                regions.add(in_fields[3])

            if in_fields[4] and (in_fields[4] != '\\N'):
                langs.add(in_fields[4])

            if in_fields[5] and (in_fields[5] != '\\N'):
                title_types_dict = collect_title_title_types(title_types_dict, in_fields, ttl_ttl_type_writer)

    # Now we can write out our role master data to file:
    for key, val in title_types_dict.items():
        # we reverse the dict keys/values here:
        ttl_type_writer.writerow({'title_type_id': val, 'title_type_name': key})

    for region in regions:
        region_writer.writerow({'region_id': region, 'region_name': ''})

    for lang in langs:
        language_writer.writerow({'language_id': lang, 'language_name': ''})

    print(' - processed 100% of file.')

    ttl_aka.close()
    ttl_ttl_type.close()
    ttl_type.close()
    region_f.close()
    language_f.close()

# -------------------------------------------------------------------------

# As well as populating the title-title-types file, we gather a master collection of title
# types with unique IDs auto-assigned.
def collect_title_title_types(title_types_dict, in_fields, ttl_ttl_type_writer):
    title_types = in_fields[5].split('\x02')
    index = 0
    for title_type in title_types:
        title_type = title_type.strip()
        index += 1
        if title_type and (title_type != '\\N'):
            if not (title_type in title_types_dict):
                title_types_dict[title_type] = len(title_types_dict) + 1
            ttl_ttl_type_writer.writerow({'title_id': in_fields[0], 'title_type_id': title_types_dict[title_type], 'order': in_fields[1]})
    return title_types_dict

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

# We rename the imdb "title type" to "content type" (movie, episode, etc.) to disambiguate from the other
# title types file we already have. EIDR uses the term "referent type", but few probably understand that
# outside of EIDRland.
#
# We also split out genres.

def normalize_title_basics():
    in_file_name = 'title_basics.tsv'
    content_types_dict = {} # key is a name and value is an auto-assigned ID
    genres_dict = {} # key is a name and value is an auto-assigned ID
    print("Processing data in " + in_file_name + ".")

    # output files:
    ttl_base = io.open('csv/title.csv', mode='w', encoding='utf-8', newline='')
    ttl_base_fields = ['title_id', 'content_type_id', 'primary_title', 'original_title', 'is_adult', 'start_year', 'end_year', 'runtime_minutes']
    ttl_base_writer = csv.DictWriter(ttl_base, fieldnames=ttl_base_fields)
    ttl_base_writer.writeheader()

    cntnt_type = io.open('csv/content_type.csv', mode='w', encoding='utf-8', newline='')
    cntnt_type_fields = ['content_type_id', 'content_type_name']
    cntnt_type_writer = csv.DictWriter(cntnt_type, fieldnames=cntnt_type_fields)
    cntnt_type_writer.writeheader()

    genre_f = io.open('csv/genre.csv', mode='w', encoding='utf-8', newline='')
    genre_fields = ['genre_id', 'genre_name']
    genre_writer = csv.DictWriter(genre_f, fieldnames=genre_fields)
    genre_writer.writeheader()

    ttl_genre = io.open('csv/title_genre.csv', mode='w', encoding='utf-8', newline='')
    ttl_genre_fields = ['title_id', 'genre_id', 'order']
    ttl_genre_writer = csv.DictWriter(ttl_genre, fieldnames=ttl_genre_fields)
    ttl_genre_writer.writeheader()

    count = line_count(in_file_name)

    with io.open(in_file_name, mode='r', encoding='utf-8') as f_in:
        i = 0
        next(f_in) # ignore 1st row's headings - we already have our new custom ones.
        for line in f_in:
            i += 1
            j = (i/count) * 100
            print(f' - processing file [%d%%]' % (j), end="\r")

            in_fields = line.strip().split('\t')

            if in_fields[1] and (in_fields[1] != '\\N'):
                content_types_dict = collect_content_types(in_fields[1], content_types_dict, cntnt_type_writer)

            if in_fields[7]:
                in_fields[7] = in_fields[7].strip()

            if in_fields[8] and (in_fields[8] != '\\N'):
                genres_dict = collect_title_genres(in_fields[8], genres_dict, genre_writer, in_fields[0], ttl_genre_writer)

            ttl_base_writer.writerow({'title_id': in_fields[0], 'content_type_id': content_types_dict[in_fields[1]], 'primary_title': in_fields[2], 'original_title': in_fields[3], 'is_adult': in_fields[4], 'start_year': in_fields[5], 'end_year': in_fields[6], 'runtime_minutes': in_fields[7]})

    print(' - processed 100% of file.')

    ttl_base.close()
    cntnt_type.close()
    genre_f.close()
    ttl_genre.close()

# -------------------------------------------------------------------------

def collect_content_types(content_type, content_types_dict, cntnt_type_writer):
    if not (content_type in content_types_dict):
        content_types_dict[content_type] = len(content_types_dict) + 1
        clean_content_type = content_type.replace('tv', 'TV ')
        cntnt_type_writer.writerow({'content_type_id': content_types_dict[content_type], 'content_type_name': clean_content_type})
    return content_types_dict

# -------------------------------------------------------------------------

def collect_title_genres(genre_string, genres_dict, genre_writer, title_id, ttl_genre_writer):
    genres = genre_string.split(',')
    index = 0
    for genre in genres:
        genre = genre.strip()
        index += 1
        if genre and (genre != '\\N'):
            if not (genre in genres_dict):
                genres_dict[genre] = len(genres_dict) + 1
                genre_writer.writerow({'genre_id': genres_dict[genre], 'genre_name': genre})
            ttl_genre_writer.writerow({'title_id': title_id, 'genre_id': genres_dict[genre], 'order': index})
    return genres_dict

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

# Title principals - which talent (cast & crew) is in which movies. Split out category to a master table, but not job,
# and not then character names (role names) field.
#
# This file has an odd structure - no tabs, just spaces. We do what we can to use as much as possible. But we don't have
# to be too pedantic. If we lose some rows, that's OK for our sample data needs.
#

def normalize_title_principals():
    in_file_name = 'title_principals.tsv'
    categories_dict = {} # key is a name and value is an auto-assigned ID
    print("Processing data in " + in_file_name + ".")

    # output files:
    ttl_prins = io.open('csv/title_principal.csv', mode='w', encoding='utf-8', newline='')
    ttl_prins_fields = ['title_id', 'talent_id', 'order', 'category_id', 'job', 'role_names']
    ttl_prins_writer = csv.DictWriter(ttl_prins, fieldnames=ttl_prins_fields)
    ttl_prins_writer.writeheader()

    cats = io.open('csv/category.csv', mode='w', encoding='utf-8', newline='')
    cats_fields = ['category_id', 'category_name']
    cats_writer = csv.DictWriter(cats, fieldnames=cats_fields)
    cats_writer.writeheader()

    count = line_count(in_file_name)

    with io.open(in_file_name, mode='r', encoding='utf-8') as f_in:
        dupe_keys = set()
        i = 0
        next(f_in) # ignore 1st row's headings - we already have our new custom ones.
        for line in f_in:
            i += 1
            j = (i/count) * 100
            print(f' - processing file [%d%%]' % (j), end="\r")

            # for some reason, this input file does not have tabs, just multi-spaces.
            # Also, some of the fields are only separated by one space. Do what we can.

            # We know these can be fixed - so fix them:
            line = line.strip().replace(' \\N ', '  \\N  ')

            # collapse multiple consecutive spaces to one tab:
            in_fields = re.sub('  +', '\t', line).split('\t')

            # After the above hacks, play it safe - some records will be filtered out here:
            if len(in_fields) == 6 and in_fields[0] and in_fields[1] and in_fields[2] and (
            in_fields[0] != '\\N') and (in_fields[1] != '\\N') and (in_fields[2] != '\\N'):

                key_val = in_fields[0] + '-' + in_fields[1] + '-' + in_fields[2]
                if not key_val in dupe_keys:
                    dupe_keys.add(key_val)

                    catgy = in_fields[3]
                    if catgy and catgy != '\\N':
                        categories_dict = collect_categories(catgy, categories_dict, cats_writer)

                    characs = in_fields[5]
                    if len(characs) > 180:
                        # there are some very long strings here, and we are not splitting them.
                        # The length of 180 is slightly less than the related database field, to
                        # accommodate any extra double-quotes which are introduced by the CSV writer.
                        characs = '\\N'

                    # the 'characters' field is a bit messy example: "[""\""Sie\"", Lia Lona, Schauspielerin""]"
                    # so we try to simplify for our simple needs:
                    if characs != '\\N':
                        characs = characs.replace('"', '').replace('[', '').replace(']', '').replace('\\', '')
                        # now the above example becomes this: "Sie, Lia Lona, Schauspielerin"

                    catgy_id = '\\N'
                    if catgy in categories_dict:
                        catgy_id = categories_dict[catgy]

                    ttl_prins_writer.writerow({'title_id': in_fields[0], 'talent_id': in_fields[2], 'order': in_fields[1], 'category_id': catgy_id, 'job': in_fields[4], 'role_names': characs})

    print(' - processed 100% of file.')

    ttl_prins.close()
    cats.close()

# -------------------------------------------------------------------------

def collect_categories(category, categories_dict, cats_writer):
    if not (category in categories_dict):
        categories_dict[category] = len(categories_dict) + 1
        cats_writer.writerow({'category_id': categories_dict[category], 'category_name': category})
    return categories_dict

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

# Title episodes - maps episode IDs to their parent IDs, typically these are series names.
#

def normalize_title_episodes():
    in_file_name = 'title_episode.tsv'
    print("Processing data in " + in_file_name + ".")

    # output files:
    ttl_epis = io.open('csv/title_episode.csv', mode='w', encoding='utf-8', newline='')
    ttl_epis_fields = ['title_id', 'parent_title_id', 'season_number', 'episode_number']
    ttl_epis_writer = csv.DictWriter(ttl_epis, fieldnames=ttl_epis_fields)
    ttl_epis_writer.writeheader()

    count = line_count(in_file_name)

    with io.open(in_file_name, mode='r', encoding='utf-8') as f_in:
        i = 0
        next(f_in) # ignore 1st row's headings - we already have our new custom ones.
        for line in f_in:
            i += 1
            j = (i/count) * 100
            print(f' - processing file [%d%%]' % (j), end="\r")
            in_fields = line.strip().split('\t')
            ttl_epis_writer.writerow({'title_id': in_fields[0], 'parent_title_id': in_fields[1], 'season_number': in_fields[2], 'episode_number': in_fields[3]})
    print(' - processed 100% of file.')

    ttl_epis.close()

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

in_suffix = '.tsv.gz'

files = ['name.basics', 'title.akas', 'title.basics', 'title.crew',
        'title.episode', 'title.principals', 'title.ratings']

start = datetime.now()

print("")
print("Starting...")
print(start)
print("")

unzip_files(files)
normalize_name_basics()
normalize_title_akas()
normalize_title_basics()
normalize_title_principals()
normalize_title_episodes()

end = datetime.now()

print("")
print("Finished!")
print(end)
print("")
duration = end - start

minutes = divmod(duration.total_seconds(), 60)[0]

print("Duration in minutes: " + str(minutes))
print("")
