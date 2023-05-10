# IMPORT Anagrafica prodotti EDIEL - RECORD LIA

# import dei moduli necessari
import pyodbc, sys, os, shutil
path = "T:/ediel/ANPROD"            #Percorso contenente i files da importare
path2 = "T:/ediel/ANPROD_SAVE"      #Percorso contenente i files IMPORTATI
#path = "C:/Users/CED/Desktop/CED/EDI/ANPROD"            #Percorso contenente i files da importare
#path2 = "C:/Users/CED/Desktop/CED/EDI/ANPROD_SAVE"      #Percorso contenente i files IMPORTATI
report = 'ANPROD_Report.txt'                            #File riepilogo import

if os.path.isfile(report):                  #Se il file report esiste lo rimuove
        os.remove(report)

# Rimuove duplicati tabella ANPROD
def rm_duplicati():
        # Connessione al DB
            con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxxx')
            cur = con.cursor()
            db_cmd = ("WITH CTE (CODCU,CODFORTU, DuplicateCount) AS (SELECT [CODCU],[CODFORTU],ROW_NUMBER() OVER(PARTITION BY CODCU,CODFORTU ORDER BY CODFORTU) AS DuplicateCount FROM [DBProjectSyncBI].[dbo].[ANPROD_EDIEL])DELETE FROM CTE WHERE DuplicateCount > 1")
            cur.execute(db_cmd)
            cur.commit()
            con.close() #Chiusura DB

#Crea lista dei file da importare
def file_anagrafica():
    if os.listdir ( path ) == []:           #Verifica se la directory vuota
        print 'Directory vuota, nessun file da importare.\n'
        sys.exit(0)
    
    dirs = os.listdir( path )               #Lista la directory
    for file in dirs:
        print ('- ')+file
        ANAGRAFICA = file
        importa(ANAGRAFICA)                 #Richiama funzione di import e passa nome file da importare
        src = (path+"/%s" % ANAGRAFICA)     #Sorgente
        dst = (path2+"/%s" % ANAGRAFICA)    #Destinazione
        print "Sposto %s da %s a %s" %  (ANAGRAFICA, path, path2)
        shutil.move(src, dst)               #Sposta il file importato su path2
    sys.exit(0)
    
# Import da txt
def importa(ANAGRAFICA):
    print 'Inizio import'
    contalinea = 0
    report_file = open(report, 'a')
    report_file.write('Import %s' % ANAGRAFICA)
    nfile = open((path+"\%s" % ANAGRAFICA), 'r')
    lines = nfile.readlines()
    lines2 = lines[2:]                      #Salta le prime 2 righe di TESTATA
        
    for line in lines2:
        line = line.replace("'", " ")       #Toglie eventuali caratteri apici
        #print line
                                            #Tracciato EDIEL
        TIPOREC = line[0:3]                 #Fisso LIA - Obb
        if (TIPOREC != 'LIA'):
            continue
        else:
            contalinea = contalinea+1       #Conta righe importate
            CODCU = (line[3:38].strip())    #EAN - Obb
            TIPCODCU = line[38:40]          #Fisso EN - Fac
            CODEANTU = line[40:76]          #EAN - Fac
            CODFORTU = line[76:111]         #Codice ART. fornitore - Obb
            CODDISTU = line[111:146]        #Codice ridistributori (nel caso di produttori = CODFORTU) - Obb
            CODCAT = line[146:166]          #Cat. merceologica Ediel - Obb
            DESCCAT = line[166:201]         #Descr. Categoria
            DESCPROD = line[201:236]        #Descrizione prodotto - Obb
            NPZCART = line[236:251]         #N. PZ. per cartone - Fac
            NCTSTRATO = line[251:266]       #N. cartoni per strato - Fac
            NCTPERBANC = line[266:281]      #N. cartoni per bancale - Fac
            QTAMINORD = line[281:296]       #Q.ta minima ordinabile - Obb
            UDMQTAMINORD = line[296:299]    #Unita misura qta minima ordine (CT = cartone, PCE = pezzi, KGM = kg, MTR = metri, LTR = litri, CU = consumer unit, TU = trade unit - tipicamente PCE) - Obb
            MULTINCR = line[299:304]        #Multipli di incremento qta ordinata - Obb
            PRZUNI1 = line[304:319]         #Prezzo unitario - Obb
            TIPOPRZUNI1 = line[319:322]     #Tipo prezzo (AAA = prezzo netto, AAB = prezzo lordo, Fisso AAB) - Obb (se presente unitario)
            VLTPRZUNI1 = line[322:325]      #Codice ISO divisa (tipicamente EUR) - Obb
            UDMPRZUN1 = line[325:328]       #Unita misura prezzo unitario (vedi UDMQTAMINORD, tipicamente PCE) - Obb
            PRZUNI2 = line[328:343]         #Prezzo vendita consigliato - Fac
            TIPOPRZUNI2 = line[343:346]     #Tipo prezzo (AAA = prezzo netto, AAB = prezzo lordo, Fisso AAA) - Fac
            VLTPRZUNI2 = line[346:349]      #Valuta prezzo unitario (obbligatorio se presente prezzo unitario - Fisso EUR) - Fac
            UDMPRZUN2 = line[349:352]       #Unita misura prezzo unitario (vedi UDMQTAMINORD, obbligatorio se presente prezzo unitario - tipicamente PCE) - Fac
            DATAINORD = line[352:360]       #Data inizio ordinabilita - Fac
            DATAFIORD = line[360:368]       #Data fine ordinabilita - Fac
            TIPOPROD = line[368:369]        #Tipo prodotto (1 = Normale, 2 = Kit, 3 = componente non ordinabile) - Obb
            PROMO = line[369:370]           #Prodotto promozionale - Fac
            CODCOM = line[370:405]          #Codice Modello - Obb
            MARCAPROD = line[405:440]       #Marca prodotto - Obb
            ALIQIVA = line[440:447]         #Aliquota IVA - Obb
            CODESIVA = line[447:450]        #Cod. esenzione IVA (Reverse Charge = A03) - Fac
            LARGPROD = line[450:465]        #Larghezza prodotto imballato - Obb
            ALTEZPROD = line[465:480]       #Altezza prodotto imballato - Obb
            PROFPROD = line[480:495]        #Profondita prodotto imballato - Obb
            LARNPROD = line[495:510]        #Larghezza prodotto senza imballo - Fac
            ALTENPROD = line[510:525]       #Altezza prodotto senza imballo - Fac
            PRONPROD = line[525:540]        #Profondita prodotto senza imballo - Fac
            UMDIMENS = line[540:543]        #Unita misura dimenzioni (Fisso CMT) - Obb
            PESOLPROD = line[543:551]       #Peso prodotto imballato - Obb
            PESONPROD = line[551:559]       #Peso prodotto senza imballo - Fac
            UMPESO = line[559:562]          #Unita misura peso (Fisso KGM) - Obb
            NSOTIMB = line[562:566]         #N. sotto imballi cartone - Fac
            DATAINIDISP = line[566:574]     #Data inizio disponibilita prodotto - Fac
            CODFORSOST = line[574:609]      #Cod. prodotto sostituito con campo CODFORTU - Fac
            STRATPALLET = line[619:611]     #N. max. strati pallet - Fac
            GARANZIA = line[611:614]        #Mesi forniti garanzia Produttore
            RAEE = line[614:615]            #Prodotto soggetto a RAEE (N/Y) - Obb
            IMPRAEE = line[615:622]         #Valore RAEE (Obb se RAEE = Y) - Obb
            SIAE = line[622:623]            #Prodotto soggetto a SIAE (N/Y) - Obb
            IMPSIAE = line[623:630]         #Valore SIAE (Obb se SIAE = Y) - Obb
                                            #Fine tracciato EDIEL
            #print ('Import del seguente articolo EAN '+CODCU+'\n')
            #Verifica campi vuoti
            if CODCU == '':
                CODCU = '0000000000000'
            if TIPCODCU == '':
                TIPCODCU = 'NULL'
            if CODEANTU == '':
                CODEANTU = '0000000000000'
            if CODFORTU == '':
                CODFORTU = 'NULL'
            if CODDISTU == '':
                CODDISTU = 'NULL'
            if CODCAT == '':
                CODCAT = 'NULL'
            if DESCCAT == '':
                DESCCAT = 'NULL'
            if DESCPROD == '':
                DESCPROD = 'NULL'
            if NPZCART == '':
                NPZCART = 'NULL'
            if NCTSTRATO == '':
                NCTSTRATO = 'NULL'
            if NCTPERBANC == '':
                NCTPERBANC = 'NULL'
            if QTAMINORD == '':
                QTAMINORD = 'NULL'
            if UDMQTAMINORD == '':
                UDMQTAMINORD = 'NULL'
            if MULTINCR == '':
                MULTINCR = '000000'
            if PRZUNI1 == '':
                PRZUNI1 = 'NULL'
            if TIPOPRZUNI1 == '':
                TIPOPRZUNI1 = 'NULL'
            if VLTPRZUNI1 == '':
                VLTPRZUNI1 = 'NULL'
            if UDMPRZUN2 == '':
                UDMPRZUN2 = 'NULL'
            if DATAINORD == '':
                DATAINORD = 'NULL'
            if DATAFIORD == '':
                DATAFIORD = 'NULL'
            if TIPOPROD == '':
                TIPOPROD = 'NULL'
            if PROMO == '':
                PROMO = 'NULL'
            if CODCOM == '':
                CODCOM = 'NULL'
            if MARCAPROD == '':
                MARCAPROD = 'NULL'
            if ALIQIVA == '':
                ALIQIVA = 'NULL'
            if CODESIVA == '':
                CODESIVA = 'NULL'
            if LARGPROD == '':
                LARGPROD = 'NULL'
            if ALTEZPROD == '':
                ALTEZPROD = 'NULL'
            if PROFPROD == '':
                PROFPROD = 'NULL'
            if LARNPROD == '':
                LARNPROD = 'NULL' 
            if ALTENPROD == '':
                ALTENPROD = 'NULL' 
            if PRONPROD == '':
                PRONPROD = 'NULL'
            if UMDIMENS == '':
                UMDIMENS = 'NULL'
            if PESOLPROD == '':
                PESOLPROD = 'NULL'
            if PESONPROD == '':
                PESONPROD = 'NULL'
            if UMPESO == '':
                UMPESO = 'NULL'
            if NSOTIMB == '':
                NSOTIMB = '0000'
            if DATAINIDISP == '':
                DATAINIDISP = 'NULL' 
            if CODFORSOST == '':
                CODFORSOST = 'NULL'
            if STRATPALLET == '':
                STRATPALLET = '00'
            if GARANZIA == '':
                GARANZIA = '000'
            if RAEE == '':
                RAEE = 'NULL' 
            if IMPRAEE == '':
                IMPRAEE = 'NULL'
            if SIAE == '':
                SIAE = 'NULL'
            if IMPSIAE == '':
                IMPSIAE = 'NULL'
            CODCU = ''.join(c for c in CODCU if c not in ('-'))
            CODCU = int(CODCU)
            #Fine verifica campi vuoti
            # Connessione al DB
            #try:
            con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxxx')
            cur = con.cursor()
            db_cmd = ("INSERT INTO dbo.ANPROD_EDIEL ([TIPOREC],[CODCU],[TIPCODCU],[CODEANTU],[CODFORTU],[CODDISTU],[CODCAT],[DESCCAT],\
            [DESCPROD],[NPZCART],[NCTSTRATO],[NCTPERBANC],[QTAMINORD],[UDMQTAMINORD],[MULTINCR],[PRZUNI1],[TIPOPRZUNI1],[VLTPRZUNI1],\
            [UDMPRZUN1],[PRZUNI2],[TIPOPRZUNI2],[VLTPRZUNI2],[UDMPRZUN2],[DATAINORD],[DATAFIORD],[TIPOPROD],[PROMO],[CODCOM],[MARCAPROD],\
            [ALIQIVA],[CODESIVA],[LARGPROD],[ALTEZPROD],[PROFPROD],[LARNPROD],[ALTENPROD],[PRONPROD],[UMDIMENS],[PESOLPROD],[PESONPROD],\
            [UMPESO],[NSOTIMB],[DATAINIDISP],[CODFORSOST],[STRATPALLET],[GARANZIA],[RAEE],[IMPRAEE],[SIAE],[IMPSIAE]) VALUES ('%s','%d',\
            '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',\
            '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (TIPOREC,\
            CODCU,TIPCODCU,CODEANTU,CODFORTU,CODDISTU,CODCAT,DESCCAT,DESCPROD,NPZCART,NCTSTRATO,NCTPERBANC,QTAMINORD,UDMQTAMINORD,MULTINCR,\
            PRZUNI1,TIPOPRZUNI1,VLTPRZUNI1,UDMPRZUN1,PRZUNI2,TIPOPRZUNI2,VLTPRZUNI2,UDMPRZUN2,DATAINORD,DATAFIORD,TIPOPROD,PROMO,CODCOM,\
            MARCAPROD,ALIQIVA,CODESIVA,LARGPROD,ALTEZPROD,PROFPROD,LARNPROD,ALTENPROD,PRONPROD,UMDIMENS,PESOLPROD,PESONPROD,UMPESO,NSOTIMB,\
            DATAINIDISP,CODFORSOST,STRATPALLET,GARANZIA,RAEE,IMPRAEE,SIAE,IMPSIAE))
            cur.execute(db_cmd)
            cur.commit()
            con.close() #Chiusura DB
       
            #except:
            #    print 'Errore durante INSERT sul DB'
            #    report_file.write('\nErrore durante INSERT sul DB') 

            #    sys.exit(1)
    
    report_file.write('Importate %s righe\n' % contalinea)        
    nfile.close() # Chiusura file
    report_file.close()
    print 'Fine import'
    print ('Importate %s righe\n' % contalinea)
    #sys.exit(0)

    

file_anagrafica()
rm_duplicati()
