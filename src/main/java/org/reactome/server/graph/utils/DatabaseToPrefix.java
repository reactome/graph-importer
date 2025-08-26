package org.reactome.server.graph.utils;

import java.util.Map;

public class DatabaseToPrefix {

    public static final Map<Long, String> mapping = Map.<Long, String>ofEntries(
            Map.entry(1L, "go"), // https://registry.identifiers.org/registry/go
            Map.entry(2L, "uniprot"), // https://registry.identifiers.org/registry/uniprot
            Map.entry(3L, "ena.embl"), // https://registry.identifiers.org/registry/ena.embl
            Map.entry(4L, "ec-code"), // https://registry.identifiers.org/registry/ec-code
            Map.entry(5L, "so"), // https://registry.identifiers.org/registry/so
            Map.entry(6L, "kegg.compound"), // https://registry.identifiers.org/registry/kegg.compound
            Map.entry(72810L, "taxonomy"), // https://registry.identifiers.org/registry/taxonomy
            Map.entry(114984L, "chebi"), // https://registry.identifiers.org/registry/chebi
            Map.entry(162345L, "kegg.glycan"), // https://registry.identifiers.org/registry/kegg.glycan
            Map.entry(181973L, "pubchem.compound"), // https://registry.identifiers.org/registry/pubchem.compound
            Map.entry(183154L, "pubchem.substance"), // https://registry.identifiers.org/registry/pubchem.substance
            Map.entry(427877L, "ncbiprotein"), // https://registry.identifiers.org/registry/ncbiprotein
            Map.entry(437354L, "mod"), // https://registry.identifiers.org/registry/mod
            Map.entry(1118111L, "cas"), // https://registry.identifiers.org/registry/cas
            Map.entry(1118112L, "umbbd.compound"), // https://registry.identifiers.org/registry/umbbd.compound
            Map.entry(1247631L, "doid"), // https://registry.identifiers.org/registry/doid
            Map.entry(1592498L, "kegg.genes"), // https://registry.identifiers.org/registry/kegg.genes
            Map.entry(1598255L, "ncbigene"), // https://registry.identifiers.org/registry/ncbigene
            Map.entry(1606610L, "mirbase"), // https://registry.identifiers.org/registry/mirbase
            Map.entry(1655447L, "cosmic"), // https://registry.identifiers.org/registry/cosmic
            Map.entry(2473614L, "cl"), // https://registry.identifiers.org/registry/cl
            Map.entry(3209110L, "nucleotide"), // https://registry.identifiers.org/registry/nucleotide
            Map.entry(4612643L, "mim"), // https://registry.identifiers.org/registry/mim
            Map.entry(5263705L, "knapsack"), // https://registry.identifiers.org/registry/knapsack
            Map.entry(5263706L, "wikipedia.en"), // https://registry.identifiers.org/registry/wikipedia.en
            Map.entry(5263712L, "chemspider"), // https://registry.identifiers.org/registry/chemspider
            Map.entry(5334734L, "orcid"), // https://registry.identifiers.org/registry/orcid
            Map.entry(9016465L, "iuphar.ligand"), // https://registry.identifiers.org/registry/iuphar.ligand
            Map.entry(9651084L, "clinvar"), // https://registry.identifiers.org/registry/clinvar
            Map.entry(9704750L, "uberon"), // https://registry.identifiers.org/registry/uberon
            Map.entry(9708210L, "rnacentral"), // https://registry.identifiers.org/registry/rnacentral
            Map.entry(9751165L, "ncit"), // https://registry.identifiers.org/registry/ncit
            Map.entry(9770671L, "rhea"), // https://registry.identifiers.org/registry/rhea
            Map.entry(9795005L, "pharmgkb.pathways"), // https://registry.identifiers.org/registry/pharmgkb.pathways
            Map.entry(9828336L, "eco"), // https://registry.identifiers.org/registry/eco
            Map.entry(9846591L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(9954034L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10072345L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10187906L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10296127L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10410424L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10526600L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10588185L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10664382L, "ensembl"), // https://registry.identifiers.org/registry/ensembl
            Map.entry(10762538L, "ensembl.metazoa"), // https://registry.identifiers.org/registry/ensembl.metazoa
            Map.entry(10762539L, "fb"), // https://registry.identifiers.org/registry/fb
            Map.entry(10850496L, "ensembl.metazoa"), // https://registry.identifiers.org/registry/ensembl.metazoa
            Map.entry(10850497L, "wb"), // https://registry.identifiers.org/registry/wb
            Map.entry(10930830L, "ensembl.protist"), // https://registry.identifiers.org/registry/ensembl.protist
            Map.entry(10930831L, "dictybase.gene"), // https://registry.identifiers.org/registry/dictybase.gene
            Map.entry(10983698L, "ensembl.fungi"), // https://registry.identifiers.org/registry/ensembl.fungi
            Map.entry(10983699L, "pombase"), // https://registry.identifiers.org/registry/pombase
            Map.entry(11014825L, "ensembl.fungi"), // https://registry.identifiers.org/registry/ensembl.fungi
            Map.entry(11014826L, "sgd"), // https://registry.identifiers.org/registry/sgd
            Map.entry(11045638L, "ensembl.protist"), // https://registry.identifiers.org/registry/ensembl.protist
            Map.entry(11045639L, "plasmodb"), // https://registry.identifiers.org/registry/plasmodb
            Map.entry(11066634L, "intact"), // https://registry.identifiers.org/registry/intact
            Map.entry(11318304L, "cosmic"), // https://registry.identifiers.org/registry/cosmic
            Map.entry(11331667L, "ctd.gene"), // https://registry.identifiers.org/registry/ctd.gene
            Map.entry(11352691L, "dbsnp"), // https://registry.identifiers.org/registry/dbsnp
            Map.entry(11693127L, "iuphar.ligand"), // https://registry.identifiers.org/registry/iuphar.ligand
            Map.entry(11693343L, "iuphar.receptor"), // https://registry.identifiers.org/registry/iuphar.receptor
            Map.entry(11696175L, "genecards"), // https://registry.identifiers.org/registry/genecards
            Map.entry(11727460L, "hgnc"), // https://registry.identifiers.org/registry/hgnc
            Map.entry(11748909L, "hpa"), // https://registry.identifiers.org/registry/hpa
            Map.entry(11764435L, "ec-code"), // OLD IntEnz // https://registry.identifiers.org/registry/ec-code
            Map.entry(11765832L, "kegg"), // https://registry.identifiers.org/registry/kegg
            Map.entry(11834425L, "mgi"), // https://registry.identifiers.org/registry/mgi
            Map.entry(11923311L, "ncbigene"), // https://registry.identifiers.org/registry/ncbigene
            Map.entry(12031514L, "orphanet"), // https://registry.identifiers.org/registry/orphanet
            Map.entry(12034887L, "pdb"), // https://registry.identifiers.org/registry/pdb
            Map.entry(12126520L, "pro"), // https://registry.identifiers.org/registry/pro
            Map.entry(12185724L, "rgd"), // https://registry.identifiers.org/registry/rgd
            Map.entry(12194977L, "refseq"), // https://registry.identifiers.org/registry/refseq
            Map.entry(12589136L, "rhea"), // https://registry.identifiers.org/registry/rhea
            Map.entry(12689998L, "vgnc"), // https://registry.identifiers.org/registry/vgnc
            Map.entry(12709342L, "xenbase"), // https://registry.identifiers.org/registry/xenbase
            Map.entry(12714957L, "zfin"), // https://registry.identifiers.org/registry/zfin
            Map.entry(12719374L, "zinc"), // https://registry.identifiers.org/registry/zinc
            Map.entry(12735880L, "hmdb"), // https://registry.identifiers.org/registry/hmdb
            Map.entry(12736502L, "biomodels.db") // https://registry.identifiers.org/registry/biomodels.db
    );
}
