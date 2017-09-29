#! /bin/bash

printf "@prefix :      <http://purl.bdrc.io/ontology/core/> .\n" > $2
printf "@prefix adm:   <http://purl.bdrc.io/ontology/admin/> .\n" >> $2
printf "@prefix bdd:   <http://purl.bdrc.io/data/> .\n" >> $2
printf "@prefix bdr:   <http://purl.bdrc.io/resource/> .\n" >> $2
printf "@prefix owl:   <http://www.w3.org/2002/07/owl#> .\n" >> $2
printf "@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" >> $2
printf "@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .\n" >> $2
printf "@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .\n" >> $2
printf "@prefix tbr:   <http://purl.bdrc.io/ontology/toberemoved/> .\n" >> $2
printf "@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .\n" >> $2
printf "@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .\n\n" >> $2

cd $1

if [ `ls -1 $1/*.ttl 2>/dev/null | wc -l ` -gt 0 ];
then
    for i in *.ttl ; do
      nm=`echo "$i" | cut -d"." -f1` ;
      printf "graph bdr:$nm {" >> $2 ;
      grep -v "^@prefix" $i >> $2 ;
      printf "}\n\n" >> $2
    done
else
    for j in * ; do
        pushd $j
        for i in *.ttl ; do
          nm=`echo "$i" | cut -d"." -f1` ;
          printf "graph bdr:$nm {" >> $2 ;
          grep -v "^@prefix" $i >> $2 ;
          printf "}\n\n" >> $2
        done
        popd
    done
fi