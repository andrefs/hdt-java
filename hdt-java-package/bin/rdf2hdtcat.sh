#!/bin/bash

function showhelp {
	echo
	echo "Script to serialize a big RDF file in n-triples format into HDT"
	echo "It splits the file in 2^N parts, compress each one with rdf2hdt, and merges them iteratively with hdtCat."
	echo
	echo "Usage $0 [OPTION]"
	echo
    echo "  -c, --catscript location of hdtCat script  (assuming it's in PATH by by default)"
    echo "  -i, --input     input file (input.rdf by default)"
	echo "  -h, --help      shows this help and exits"
	echo "  -n, --number    number of files to split FILE (2 by default)"
    echo "  -o, --output    output file (output.hdt by default)"
    echo "  -p, --parallel  number of threads to serialize RDF into HDT in parallel (1 by default)"
    echo "  -r, --rdf2hdt   location of rdf2hdt script (assuming it's in PATH by default)"
    echo "  -z, --compress  use Gzip (for input and splits; default false)"
    echo "  -g, --gzip      gzip binary (default gzip, can be other e.g. pigz)"
	echo
}

# Defaults
declare rdf2hdt="bin/rdf2hdt.sh"
declare hdtCat="bin/hdtCat.sh"
declare input="input.rdf"
declare -i lines
declare output="output.hdt"
declare -i splits=2
declare -i threads=1
declare compress=""
declare gzip="gzip"

getopt --test > /dev/null
if [[ $? -eq 4 ]]; then
    # enhanced getopt works
    OPTIONS=c:i:hn:o:p:r:zg:
    LONGOPTIONS=cat:,input:,help,number:,output:,parallel:,rdf2hdt,compress,gzip
    COMMAND=$(getopt -o $OPTIONS -l $LONGOPTIONS -n "$0" -- "$@")
    if [[ $? -ne 0 ]]; then
    	exit 2
    fi
    eval set -- "$COMMAND"
else
	echo "Enhanced getopt not supported. Brace yourself, this is not tested, but it should work :-)"
fi

while true; do
	case "$1" in
        -c|--cat)
            hdtCat=$2
            shift 2
            ;;
        -i|--input)
            input=$2
            shift 2
            ;;
		-n|--number)
			splits=2**$2
			shift 2
			;;
        -o|--output)
            output=$2
            shift 2
            ;;
        -p|--parallel)
            threads=$2
            shift 2
            ;;
        -r|--rdf2hdt)
            rdf2hdt=$2
            shift 2
            ;;
        -z|--compress)
            compress="true"
            shift 1
            ;;
        -g|--gzip)
            gzip=$2
            shift 2
            ;;
		--)
			shift
			break
			;;
		*)
			showhelp
			exit 0
			;;
	esac
done

echo "***************************************************************"
echo "Counting lines in '$input'"
echo "***************************************************************"
if [[ -z "$compress" ]]; then
    total_lines=$(wc -l < $input)
else
    total_lines=$(pv $input | $gzip -d | wc -l)
fi
lines=($total_lines+$splits-1)/$splits #Set number of lines rounding up
echo Total lines: $total_lines
echo Lines per chunk: $lines

echo rdf2hdtcat.sh $JAVA_OPTIONS

echo "***************************************************************"
echo "Splitting '$input' in $splits chunks"
echo "***************************************************************"
if [[ -z "$compress" ]]; then
    split -l $lines $input "$input"_split_
else
    pv "$input" | $gzip -d | split -l $lines - "$input"_split_ --filter="$gzip > \$FILE.gz"
fi

echo "***************************************************************"
echo "Serializing into HDT $splits files using $threads threads"
echo "***************************************************************"
if [[ -z "$compress" ]]; then
    echo -n "$input"_split_* | xargs -I{} -d' ' -P$threads bash -c "JAVA_OPTIONS='-Xmx4g' $rdf2hdt -rdftype ntriples {} {}_"$splits".hdt"
else
    echo -n "$input"_split_*.gz | sed 's/\.gz\( \|$\)/ /g' | xargs -I{} -d' ' -P$threads bash -c "JAVA_OPTIONS='-Xmx8g' $rdf2hdt -rdftype ntriples <(cat {}.gz | $gzip -d) {}_$splits.hdt"
fi

for (( i=$splits; i>1; i=i/2 )); do
    echo "***************************************************************"
    echo "Merging $i hdt files: " "$input"_split_*_"$i".hdt
    echo "***************************************************************"
    echo -n "$input"_split_*_"$i".hdt | xargs -d' ' -n2 -P$threads bash -c 'temp=${2%_*.hdt} ; bin/hdtCat.sh $1 $2 ${1%_*.hdt}_${temp#*split_}_$0.hdt' $((i/2))
    command='temp=${2%_*.hdt} ; '$hdtCat' $1 $2 ${1%_*.hdt}_${temp#*split_}_$0.hdt'
    echo -n "$input"_split_*_"$i".hdt | xargs -d' ' -n2 -P$threads bash -c "$command" $((i/2))
done

echo "***************************************************************"
echo "Moving output to '$output' file"
echo "***************************************************************"
mv "$input"_split*_1.hdt "$output"

echo "***************************************************************"
echo "Cleaning up split files"
echo "***************************************************************"
rm "$input"_split_*
