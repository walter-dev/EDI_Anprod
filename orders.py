# -*- coding: cp1252 -*-
# Orders EDI
# import dei moduli necessari
import pyodbc, string, time, os, sys, re

def cre_ordine():
    # Log errori
    LOG_ERRORI = 'errori.txt'
    ERRORI = 0
    # Percorso ordini creati
    path = "ord\\"

    # Lettura Numero Assoluto
    NOMEFILE = 'numass.txt'         # Nome del file contenente Numero Ass.
    nfile = open(NOMEFILE, "r")
    lines = nfile.readlines()
    lines2 = list(set(lines))       # Elimina Num. ass. doppioni

    for line in lines2:
        NUMASS1 = str(line)
        NUMASS2 = "".join(c for c in NUMASS1 if c not in ('\n',']','[','\''))
        
        print ('N. Assoluto = %s:' % NUMASS2)

    #Inizio TESTATA
    # Record BGM - Informazioni INIZIO
        # Connessione al DB
        con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxx')
        cur = con.cursor()
        db_cmd = ("SELECT TOP 1 [NUMASS],[CLIFOR],[DATORO],[NUMORO],[CODMIT] FROM dbo.MOVMAGAZZINO LEFT JOIN dbo.FORN_EDI ON CLIFOR = CODCLI where NUMASS = '%s'" % NUMASS2)
        res = cur.execute(db_cmd)

        # Estraggo dati restituiti dalla select
        for row in res:
            NUMASS=row.NUMASS       # N. Assoluto
            COD_FOR=row.CLIFOR      # Codice Fornitore
            ID_EDI_DEST_1=row.CODMIT # GLN Destinatario FF
            #NUMDOC=row.NUMDOC       # NUMDOC Numero Ordine
            #DATDOC=row.DATDOC       # DATADOC Data Ordine
            NUMDOC=row.NUMORO       # Numero ordine originale
            DATDOC=row.DATORO       # Data ordine originale
            
        con.close() # Chiusura DB

        # Verifica campi vuoti
        if NUMASS == '': print 'N. Assoluto VUOTO'
        if COD_FOR == '': print 'Codice Fornitore VUOTO'
        if NUMDOC == '': print 'NUMDOC Numero Ordine VUOTO'
        if DATDOC == '': print 'DATADOC Data Ordine VUOTO'
        if ID_EDI_DEST_1 == '': print 'GLN Destinatario FF VUOTO'

        # Converto in stringa
        NUMASS = str(NUMASS)
        COD_FOR = str(COD_FOR)
        ID_EDI_DEST_1 = str(ID_EDI_DEST_1)
        NUMDOC = str(NUMDOC)
        DATDOC = str(DATDOC)
        DATDOC = ''.join(c for c in DATDOC if c not in ('-'))
        # Valori Fissi
        TIPOREC = 'BGM'                 # Tipo record
        global ID_EDI_MITT_1
        ID_EDI_MITT_1 = '00798870879'   # P.IVA o GLN Mittente
        ID_EDI_MITT_2 = 'ZZ'            # Qualificatore del campo sopra (ZZ x P.I. EN se GLN)
        ID_EDI_DEST_2 = 'ZZ'
        # Conta n. caratteri
        count = len(re.findall('[0123456789]', ID_EDI_DEST_1))
        if count > 11: ID_EDI_DEST_2 = 'EN'            # Qualificatore del campo GLN_DEST (ZZ x P.I. EN se GLN)
        
        TIPODOC = 'ORDERS'              # Ordine
        ECCEZIONE = '0' # Eccezione per Ingrammicro invia gli EAN al posto del CODFORTU
        print ('Fornitore %s ' % COD_FOR)
        if (COD_FOR == 'FF0001282'): ECCEZIONE = '1'
        print '%s Ordine N. %s' ' del ''%s \n ' % (TIPOREC,NUMDOC, DATDOC[0:8])
        
        DATA = time.strftime("%d%m%Y%H%M%S")

        # Crea file ordine
        NOME_FILE = (path+'ordine.%s.%s.%s.txt' % (ID_EDI_MITT_1, NUMDOC, DATA))
        FILE = open(NOME_FILE, 'a+')

# Scrivo Record BGM
        FILE.write('%s%-35s%-18s%-35s%-18s%-6s%-35s%s' % (TIPOREC,ID_EDI_MITT_1,ID_EDI_MITT_2,ID_EDI_DEST_1,ID_EDI_DEST_2,TIPODOC,NUMDOC,DATDOC[0:8]+'\n'))
        FILE.close()
        
    # Record NAS - Informazioni sul fornitore
        # Connessione al DB
        con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxx')
        cur = con.cursor()
        db_cmd = ("SELECT TOP 1 [CODMIT],[RAGSO1],[INDIRI],[CITTAC],CLI.[PROVIN],[CAPCLI],[CODNAZ] FROM dbo.MOVMAGAZZINO LEFT JOIN dbo.CLIENTI AS CLI ON CLIFOR = CLI.CODCLI LEFT JOIN dbo.FORN_EDI AS FORN ON CLIFOR = FORN.CODCLI WHERE NUMASS = '%s'" % NUMASS2)
        res = cur.execute(db_cmd)

        # Estraggo dati restituiti dalla select
        for row in res:
            CODFORN=row.CODMIT      # GLN Fornitore
            RAGSOCF=row.RAGSO1      # Ragione sociale
            INDIRF=row.INDIRI       # Indirizzo FF
            CITTAF=row.CITTAC       # Citta'
            PROVF=row.PROVIN        # Provincia
            CAPF=row.CAPCLI         # CAP
            NAZIOF=row.CODNAZ       # Codice nazione
            
        con.close() # Chiusura DB

        # Verifica campi vuoti
        if CODFORN == '': print 'GLN Fornitore VUOTO'
        if RAGSOCF == '': print 'Ragione sociale VUOTO'
        if INDIRF == '': print 'Indirizzo FF VUOTO'
        if CITTAF == '': print 'Citta VUOTO'
        if PROVF == '': print 'Provincia VUOTO'
        if CAPF == '': print 'CAP VUOTO'
        if NAZIOF == '': print 'Codice nazione VUOTO'

        # Converto in stringa
        CODFORN = str(CODFORN)
        RAGSOCF = str(RAGSOCF)
        INDIRF = str(INDIRF)
        CITTAF = str(CITTAF)
        PROVF = str(PROVF)
        CAPF = str(CAPF)
        NAZIOF = str(NAZIOF)
        # Valori Fissi
        TIPOREC = 'NAS'   # Tipo record
        QCODFORN = '14'   # Tipo codice FF (GLN = 14, P.IVA = VA)
        
        print '\n%s fornitore: %s - %s\n' % (TIPOREC,CODFORN,RAGSOCF)

        # Crea file ordine
        FILE = open(NOME_FILE, 'a+')
        # Scrivo Record NAS
        FILE.write('%-3s%-17s%-3s%-70s%-70s%-35s%-9s%-9s%-3s' % (TIPOREC,CODFORN,QCODFORN,RAGSOCF,INDIRF,CITTAF,PROVF,CAPF,NAZIOF+'\n'))
        FILE.close()

    # Record NAB - Informazioni sul mittente dell'ordine
        # Valori Fissi
        TIPOREC = 'NAB'  # Tipo record
        CODBUYER = '00798870879'   # P.IVA o GLN Mittente
        QCODBUY = 'VA'
        RAGSOCB = 'PAPINO ELETTRODOMESTICI S.P.A.' # Rag. soc. Mittente
        INDIRB = 'VIALE ASTREL,1 C.DA PALAZZOLO'
        CITTAB = 'BELPASSO'
        PROVB = 'CT'
        CAPB = '95032'
        NAZIOB = 'IT'

        print '\n%s mittente: %s - %s\n' % (TIPOREC,CODBUYER,RAGSOCB)
        
        # Crea file ordine
        FILE = open(NOME_FILE, 'a+')
        # Scrivo Record NAB
        FILE.write('%-3s%-17s%-3s%-70s%-70s%-35s%-9s%-9s%-3s' % (TIPOREC,CODBUYER,QCODBUY,RAGSOCB,INDIRB,CITTAB,PROVB,CAPB,NAZIOB+'\n'))
        FILE.close()

    # Record NAD - Informazioni sul punto di consegna della merce (Delivery Point)
    
        # Valori Fissi
        TIPOREC = 'NAD'  # Tipo record
        QCODCONS = '14'

        # Connessione al DB
        con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxx')
        cur = con.cursor()
        db_cmd = ("SELECT TOP (100) PERCENT [COD],[INDIRI],[CITTA],MAG.[PROVIN] FROM dbo.MOVMAGAZZINO LEFT JOIN dbo.GLN_PDV_EDI ON MAG = CODMAG LEFT JOIN dbo.MAGAZZINI AS MAG ON CODMAG = CODICE where NUMASS = '%s'" % NUMASS2)
        res = cur.execute(db_cmd)

        # Estraggo dati restituiti dalla select
        for row in res.fetchall():
            COD=row.COD         # N. GLN PDV
            INDIRD=row.INDIRI   # Indirizzo punto di consegna
            CITTAD=row.CITTA    # Città
            PROVD=row.PROVIN    # Provincia

        con.close() # Chiusura DB

        # Verifica campi vuoti
        if COD == '': print 'N. GLN VUOTO'
        if INDIRD == '': print 'Indirizzo VUOTO'
        if CITTAD == '': print 'Citta VUOTO'
        if PROVD == '': print 'Provinvia VUOTO'

        # Converto in stringa
        CODCONS = str(COD)
        INDIRD = str(INDIRD)
        CITTAD = str(CITTAD)
        PROVD = str(PROVD)

        print ('\n%s Codice GLN punto vendita: %s\n' % (TIPOREC,COD))
        # Crea file ordine
        FILE = open(NOME_FILE, 'a+')
        # Scrivo Record NAD
        FILE.write('%-3s%-17s%-3s%-70s%-70s%-35s%-9s\n' % (TIPOREC,CODCONS,QCODCONS,RAGSOCB,INDIRD,CITTAD,PROVD))
        FILE.close()

    # Record DTM - Informazioni sulla data di consegna

        # Valori fissi
        TIPOREC = 'DTM' # Tipo record
        TIPODATAC = '064' # Tipo DATA consegna 064 = Non consegnare prima di tale data

        # Connessione al DB
        con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxx')
        cur = con.cursor()
        db_cmd = ("SELECT TOP (100) PERCENT DTCON FROM dbo.MOVMAGAZZINO where NUMASS = '%s'" % NUMASS2)
        res = cur.execute(db_cmd)

        # Estraggo dati restituiti dalla select
        for row in res.fetchall():
            DTCON=row.DTCON 

        if DTCON == '': print 'DATA CONSEGNA VUOTO'

        # Converto in stringa
        DTCON = str(DTCON) # Data consegna
        DTCON = ''.join(c for c in DTCON if c not in ('-'))
        DATACONS = DTCON[0:8]
        
        print ('\n%s Data prevista consegna, non prima del: %s\n' % (TIPOREC,DATACONS))
        
        # Crea file ordine
        FILE = open(NOME_FILE, 'a+')
     
        # Scrivo Record DTM
        FILE.write('%-3s%-12s%s\n' %(TIPOREC,DATACONS,TIPODATAC))
        FILE.close()
        
    
    # Fine TESTATA

    # Inizio RIGHE - Record LIN
        # Valori fissi
        TIPOREC = 'LIN'     # Tipo record
        NUMRIGA = 0         # Parte da 1
        TIPOCODCU = 'EN'    # Barcode fisso
        UDMQORD = 'PCE'     # Unità misura PCE = pezzi
        TIPOPRZ = 'AAA'     # Tipo prezzo AAA = netto
        UDMPRZUN = 'PCE'    # Unità misura prezzo PCE = pezzi
        
        # Connessione al DB
        con = pyodbc.connect(driver="{SQL Server}",server='SVFSV001',database='DBProjectSyncBI',uid='sa',pwd='xxxxxxx')
        cur = con.cursor()
        db_cmd = ("SELECT DISTINCT TOP (100) PERCENT [CODFORTU],[CODALT],[DESCPROD],[QTAMOV],[INETTO],[SCONTI],[IMPLIS] FROM dbo.MOVMAGAZZINO LEFT JOIN dbo.ARTICOLI ON MOVMAGAZZINO.CODART = ARTICOLI.CODICE LEFT JOIN dbo.ANPROD_EDIEL ON ARTICOLI.CODALT = ANPROD_EDIEL.CODCU where NUMASS = '%s'" % NUMASS2)
        res = cur.execute(db_cmd)

        # Estraggo dati restituiti dalla select
        for row in res.fetchall():
            CODFORTU=row.CODFORTU   # Cod. articolo Fornitore da ANPROD
            CODEANCU=row.CODALT     # Cod. EAN ART.
            DESART=row.DESCPROD     # Descr. Art.
            QTAORD=row.QTAMOV       # Q.ta' ordinata
            PRZUNI=row.INETTO       # Importo Netto

            # Campi ALD
            SCONTI=row.SCONTI     # Sconti percentuali
            PRZBASE=row.IMPLIS  # Prezzo listino su cui calcolare sconti
            
            # Verifica campi vuoti
            
            CODFORTU = str(CODFORTU)
            
            if (CODFORTU == '' or CODFORTU == 'None' or ECCEZIONE == '1'):
                print 'ATTENZIONE CODICE ART. FORNITORE VUOTO!!!'
                CODFORTU = str(CODEANCU)
                LOG = open(LOG_ERRORI, 'a')
                LOG.write('ATTENZIONE CODICE ART. FORNITORE VUOTO - >> RECORD LIN <<  %s, Ordine N. %s' ' del ''%s - NUMASS %s\n ' % (CODEANCU,NUMDOC,DATDOC[0:8],NUMASS))
                LOG.close()
                ERRORI = 1
            print ('\n%s Articoli in ordine: EAN %s - Codice Art. Fornitore %s - %s\n' % (TIPOREC,CODEANCU,CODFORTU,DESART))
          # Creazione RIGHE
          # Converto in stringa
            CODEANCU = str(CODEANCU)
            DESART = str(DESART)
            QTAORD = str(QTAORD)
            QTAORD = "{:.3f}".format(float(QTAORD)) # Imposta 3 decimali
            QTAORD = "".join(c for c in QTAORD if c not in ('.')) # Toglie il punto
            PRZUNI = str(PRZUNI)
            PRZUNI = "{:.3f}".format(float(PRZUNI)) # Imposta 3 decimali
            PRZUNI = "".join(c for c in PRZUNI if c not in ('.')) # Toglie il punto
            NUMRIGA = NUMRIGA+1 # N. Riga incrementale
            NRIGA = str(NUMRIGA)
                
       	# Scrivo in append il dettaglio
            print('\nScrittura campo LIN ordine n. %s\n' % (NUMDOC))
            FILE = open(NOME_FILE, 'a')
            FILE.write('%-3s%-6s%-35s%-38s%-70s%-38s%-15s%-3s%-15s%-3s%-3s' % (TIPOREC,NRIGA.rjust(6,'0'),CODEANCU,TIPOCODCU,CODFORTU,DESART,QTAORD.rjust(15,'0'),UDMQORD,PRZUNI.rjust(15,'0'),TIPOPRZ,UDMPRZUN+'\n'))
            FILE.close()

    # Record ALD - Informazioni sugli sconti/maggiorazioni di dettaglio
            # Valori fissi
            TIPOREC2 = 'ALD' # Tipo record
            INDSCADD = 'A'  # Indicatore di sconto/addebito A = Sconto C = Addebito

            # Verifica campi vuoti
            if (SCONTI == '' or SCONTI == 'None'):
                print 'STRINGA SCONTI VUOTO'
                continue
            else:
                if PRZBASE == '': print 'PREZZO LISTINO VUOTO'
                PRZBASE = str(PRZBASE)
                PRZBASE = "{:.3f}".format(float(PRZBASE)) # Imposta 3 decimali
                PRZBASE = "".join(c for c in PRZBASE if c not in ('.')) # Toglie il punto
                
                # Splitto la riga sconti    
                SCONTI_VARI = SCONTI.split("+")
                for SCONTO in SCONTI_VARI:
                    SCONTO = "{:.4f}".format(float(SCONTO)) # Imposta 3 decimali
                    SCONTO = "".join(c for c in SCONTO if c not in ('.',' ','-')) # Toglie il punto, lo spazio ed il meno
                    PERC = str(SCONTO)
    
                    print ('\n%s Sconti presenti: %s\n' % (TIPOREC2,SCONTI))
                    # Crea file ordine
                    FILE = open(NOME_FILE, 'a+')
                    # Scrivo Record ALD
                    FILE.write('%-3s%-63s%s%s\n' % (TIPOREC2,INDSCADD,PERC.rjust(7,'0'),PRZBASE.rjust(15,'0')))
                    FILE.close()

        con.close() #Chiusura DB
    nfile.close() #Chiusura file numass.txt
    os.remove(NOMEFILE) #Cancella file numass.txt
    if ERRORI > 0:
        print 'Sono stati rilevati errori, verificare il log'
        raw_input("Press Enter to continue...")
cre_ordine()
