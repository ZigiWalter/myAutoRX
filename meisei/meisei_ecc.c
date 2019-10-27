
/*
 * big endian forest
 *
 * Meisei radiosondes
 * author: zilog80
 *
 */

/*
PCM-FM, 1200 baud biphase-S
1200 bit pro Sekunde: zwei Frames, die wiederum in zwei Subframes unterteilt werden koennen, d.h. 4 mal 300 bit.

Variante 1 (RS-11G ?)
<option -1>
049DCE1C667FDD8F537C8100004F20764630A20000000010040436 FB623080801F395FFE08A76540000FE01D0C2C1E75025006DE0A07
049DCE1C67008C73D7168200004F0F764B31A2FFFF000010270B14 FB6230000000000000000000000000000000000000000000001D59

0x00..0x02  HEADER  0x049DCE
0x03..0x04  16 bit  0.5s-counter, count%2=0:
0x1B..0x1D  HEADER  0xFB6230
0x20..0x23  32 bit  GPS-lat * 1e7 (DD.dddddd)
0x24..0x27  32 bit  GPS-lon * 1e7 (DD.dddddd)
0x28..0x2B  32 bit  GPS-alt * 1e2 (m)
0x2C..0x2D  16 bit  GPS-vH  * 1e2 (m/s)
0x2E..0x2F  16 bit  GPS-vD  * 1e2 (degree) (0..360 unsigned)
0x30..0x31  16 bit  GPS-vU  * 1e2 (m/s)
0x32..0x35  32 bit  date jjJJMMTT

0x00..0x02  HEADER  0x049DCE
0x03..0x04  16 bit  0.5s-counter, count%2=1:
0x17..0x18  16 bit  time ms xxyy, 00.000-59.000
0x19..0x1A  16 bit  time hh:mm
0x1B..0x1D  HEADER  0xFB6230


0x049DCE ^ 0xFB6230 = 0xFFFFFE


Variante 2 (iMS-100 ?)
<option -2>
049DCE3E228023DBF53FA700003C74628430C100000000ABE00B3B FB62302390031EECCC00E656E42327562B2436C4C01CDB0F18B09A
049DCE3E23516AF62B3FC700003C7390D131C100000000AB090000 FB62300000000000032423222422202014211B13220000000067C4

0x00..0x02  HEADER  0x049DCE
0x03..0x04  16 bit  0.5s-counter, count%2=0:
0x17..0x18  16 bit  time ms yyxx, 00.000-59.000
0x19..0x1A  16 bit  time hh:mm
0x1B..0x1D  HEADER  0xFB6230
0x1E..0x1F  16 bit  ? date (TT,MM,JJ)=(date/1000,(date/10)%100,(date%10)+10)
0x20..0x23  32 bit  GPS-lat * 1e4 (NMEA DDMM.mmmm)
0x24..0x27  32 bit  GPS-lon * 1e4 (NMEA DDMM.mmmm)
0x28..0x2A  24 bit  GPS-alt * 1e2 (m)
0x30..0x31  16 bit  GPS-vD  * 1e2 (degree)
0x32..0x33  16 bit  GPS-vH  * 1.944e2 (knots)

0x00..0x02  HEADER  0x049DCE
0x03..0x04  16 bit  0.5s-counter, count%2=1:
0x17..0x18  16 bit  1024-counter yyxx, +0x400=1024; rollover synchron zu ms-counter, nach rollover auch +0x300=768
0x1B..0x1D  HEADER  0xFB6230


Die 46bit-Bloecke sind BCH-Codewoerter. Es handelt sich um einen (63,51)-Code mit Generatorpolynom
x^12+x^10+x^8+x^5+x^4+x^3+1;
gekuerzt auf (46,34), die letzten 12 bit sind die BCH-Kontrollbits.

Die 34 Nachrichtenbits sind aufgeteilt in 16+1+16+1, d.h. nach einem 16 bit Block kommt ein Paritaetsbit,
dass 1 ist, wenn die Anzahl 1en in den 16 bit davor gerade ist, und sonst 0.
*/

/*
2 "raw" symbols -> 1 biphase-symbol (bit): 2400 (raw) baud
ecc: option_b, exact symbol rate; if necessary, adjust --br <baud>
e.g.
./meisei_ecc -1 --ecc -v -b --br 2398 audio.wav
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <math.h>
#ifdef CYGWIN
  #include <fcntl.h>  // cygwin: _setmode()
  #include <io.h>
#endif

typedef unsigned char  ui8_t;
typedef unsigned short ui16_t;
typedef unsigned int   ui32_t;
typedef short i16_t;

typedef struct {
    int jahr; int monat; int tag;
    int std; int min; int sek;
    double lat; double lon; double alt;
} datum_t;

datum_t datum;


int option_verbose = 0,  // ausfuehrliche Anzeige
    option_raw = 0,      // rohe Frames
    option_inv = 0,      // invertiert Signal
    option_res = 0,      // genauere Bitmessung
    option1 = 0,
    option2 = 0,
    option_b = 0,
    option_ecc = 0,      // BCH(63,51)
    wavloaded = 0;

float baudrate = -1;

/* -------------------------------------------------------------------------- */
// Fehlerkorrektur (noch?) nicht sehr effektiv... (t zu klein)

#include "bch_ecc.c"

int   errors;
ui8_t cw[63+1],  // BCH(63,51), t=2
      err_pos[4],
      err_val[4];
ui8_t block_err[6];
int block, check_err;

/* -------------------------------------------------------------------------- */

#define BAUD_RATE 2400  // raw symbol rate; bit=biphase_symbol, bitrate=1200

int sample_rate = 0, bits_sample = 0, channels = 0;
float samples_per_bit = 0;

int findstr(char *buff, char *str, int pos) {
    int i;
    for (i = 0; i < 4; i++) {
        if (buff[(pos+i)%4] != str[i]) break;
    }
    return i;
}

int read_wav_header(FILE *fp) {
    char txt[4+1] = "\0\0\0\0";
    unsigned char dat[4];
    int byte, p=0;

    if (fread(txt, 1, 4, fp) < 4) return -1;
    if (strncmp(txt, "RIFF", 4)) return -1;
    if (fread(txt, 1, 4, fp) < 4) return -1;
    // pos_WAVE = 8L
    if (fread(txt, 1, 4, fp) < 4) return -1;
    if (strncmp(txt, "WAVE", 4)) return -1;
    // pos_fmt = 12L
    for ( ; ; ) {
        if ( (byte=fgetc(fp)) == EOF ) return -1;
        txt[p % 4] = byte;
        p++; if (p==4) p=0;
        if (findstr(txt, "fmt ", p) == 4) break;
    }
    if (fread(dat, 1, 4, fp) < 4) return -1;
    if (fread(dat, 1, 2, fp) < 2) return -1;

    if (fread(dat, 1, 2, fp) < 2) return -1;
    channels = dat[0] + (dat[1] << 8);

    if (fread(dat, 1, 4, fp) < 4) return -1;
    memcpy(&sample_rate, dat, 4); //sample_rate = dat[0]|(dat[1]<<8)|(dat[2]<<16)|(dat[3]<<24);

    if (fread(dat, 1, 4, fp) < 4) return -1;
    if (fread(dat, 1, 2, fp) < 2) return -1;
    //byte = dat[0] + (dat[1] << 8);

    if (fread(dat, 1, 2, fp) < 2) return -1;
    bits_sample = dat[0] + (dat[1] << 8);

    // pos_dat = 36L + info
    for ( ; ; ) {
        if ( (byte=fgetc(fp)) == EOF ) return -1;
        txt[p % 4] = byte;
        p++; if (p==4) p=0;
        if (findstr(txt, "data", p) == 4) break;
    }
    if (fread(dat, 1, 4, fp) < 4) return -1;


    fprintf(stderr, "sample_rate: %d\n", sample_rate);
    fprintf(stderr, "bits       : %d\n", bits_sample);
    fprintf(stderr, "channels   : %d\n", channels);

    if ((bits_sample != 8) && (bits_sample != 16)) return -1;

    samples_per_bit = sample_rate/(float)BAUD_RATE;

    fprintf(stderr, "samples/bit: %.2f\n", samples_per_bit);

    return 0;
}


#define EOF_INT  0x1000000
unsigned long sample_count = 0;

int read_signed_sample(FILE *fp) {  // int = i32_t
    int byte, i, ret;         //  EOF -> 0x1000000

    for (i = 0; i < channels; i++) {
                           // i = 0: links bzw. mono
        byte = fgetc(fp);
        if (byte == EOF) return EOF_INT;
        if (i == 0) ret = byte;

        if (bits_sample == 16) {
            byte = fgetc(fp);
            if (byte == EOF) return EOF_INT;
            if (i == 0) ret +=  byte << 8;
        }

    }

    sample_count++;

    if (bits_sample ==  8) return ret-128;   // 8bit: 00..FF, centerpoint 0x80=128
    if (bits_sample == 16) return (short)ret;

    return ret;
}

int par=1, par_alt=1;

int read_bits_fsk(FILE *fp, int *bit, int *len) {
    int n, sample, y0;
    float l, x1;
    static float x0;

    n = 0;
    do{
        y0 = sample;
        sample = read_signed_sample(fp);
        if (sample == EOF_INT) return EOF;
        //sample_count++; // in read_signed_sample()
        par_alt = par;
        par =  (sample > 0) ? 1 : -1;
        n++;
    } while (par*par_alt > 0);

    if (!option_res) l = (float)n / samples_per_bit;
    else {                                 // genauere Bitlaengen-Messung
        x1 = sample/(float)(sample-y0);    // hilft bei niedriger sample rate
        l = (n+x0-x1) / samples_per_bit;   // meist mehr frames (nicht immer)
        x0 = x1;
    }

    *len = (int)(l+0.5);

    if (!option_inv) *bit = (1+par_alt)/2;  // oben 1, unten -1
    else             *bit = (1-par_alt)/2;  // sdr#<rev1381?, invers: unten 1, oben -1

    /* Y-offset ? */

    return 0;
}

int bitstart = 0;
double bitgrenze = 0;
/*unsigned*/ long scount = 0;
int read_rawbit(FILE *fp, int *bit) {
    int sample;
    int sum;

    sum = 0;

    if (bitstart) {
        scount = 0;    // eigentlich scount = 1
        bitgrenze = 0; //   oder bitgrenze = -1
        bitstart = 0;
    }
    bitgrenze += samples_per_bit;

    do {
        sample = read_signed_sample(fp);
        if (sample == EOF_INT) return EOF;
        //sample_count++; // in read_signed_sample()
        //par =  (sample >= 0) ? 1 : -1;    // 8bit: 0..127,128..255 (-128..-1,0..127)
        sum += sample;
        scount++;
    } while (scount < bitgrenze);  // n < samples_per_bit

    if (sum >= 0) *bit = 1;
    else          *bit = 0;

    if (option_inv) *bit ^= 1;

    return 0;
}

/* ------------------------------------------------------------------------------------ */

#define BITFRAME_LEN    1200
#define RAWBITFRAME_LEN (BITFRAME_LEN*2)

char frame_rawbits[RAWBITFRAME_LEN+10];  // braucht eigentlich nur 1/4
char frame_bits[BITFRAME_LEN+10];

#define HEADLEN 24
#define RAWHEADLEN (2*HEADLEN)

char header0x049DCE[] =                             // 0x049DCE =
"101010101011010100101011001101001100101011001101"; // 00000100 10011101 11001110
char header0x049DCEbits[] = "000001001001110111001110";
                           //111110110110001000110000
char header0xFB6230[] =                             // 0xFB6230 =
"110011001101001101001101010100101010110010101010"; // 11111011 01100010 00110000
char header0xFB6230bits[] = "111110110110001000110000";
                                                    // 0x049DCE ^ 0xFB6230 = 0xFFFFFE

char buf[RAWHEADLEN+1] = "xxxxxxxxxx\0";
int  bufpos = 0;

/* -------------------------------------------------------------------------- */

void inc_bufpos() {
  bufpos = (bufpos+1) % RAWHEADLEN;
}


char cb_inv(char c) {
    if (c == '0') return '1';
    if (c == '1') return '0';
    return c;
}

int compare_subheader() {
    int i, j;

    i = 0;
    j = bufpos;
    while (i < RAWHEADLEN) {
        if (j < 0) j = RAWHEADLEN-1;
        if (buf[j] != header0x049DCE[RAWHEADLEN-1-i]) break;
        j--;
        i++;
    }
    if (i == RAWHEADLEN) return 1;

    i = 0;
    j = bufpos;
    while (i < RAWHEADLEN) {
        if (j < 0) j = RAWHEADLEN-1;
        if (buf[j] != cb_inv(header0x049DCE[RAWHEADLEN-1-i])) break;
        j--;
        i++;
    }
    if (i == RAWHEADLEN) return 3;

    i = 0;
    j = bufpos;
    while (i < RAWHEADLEN) {
        if (j < 0) j = RAWHEADLEN-1;
        if (buf[j] != header0xFB6230[RAWHEADLEN-1-i]) break;
        j--;
        i++;
    }
    if (i == RAWHEADLEN) return 2;

    i = 0;
    j = bufpos;
    while (i < RAWHEADLEN) {
        if (j < 0) j = RAWHEADLEN-1;
        if (buf[j] != cb_inv(header0xFB6230[RAWHEADLEN-1-i])) break;
        j--;
        i++;
    }
    if (i == RAWHEADLEN) return 4;

    return 0;

}



/* -------------------------------------------------------------------------- */

int biphi_s(char* frame_rawbits, ui8_t *frame_bits) {
    int j = 0;
    int byt;

    j = 0;
    while ((byt = frame_rawbits[2*j]) && frame_rawbits[2*j+1]) {
        if ((byt < 0x30) || (byt > 0x31)) break;

        if ( frame_rawbits[2*j] == frame_rawbits[2*j+1] ) { byt = 1; }
        else                                              { byt = 0; }

        frame_bits[j] = byt;
        j++;
    }
    frame_bits[j] = 0;
    return j;
}

/* -------------------------------------------------------------------------- */
/*
ui32_t bitstr2val(char *bits, int len) {
    int j;
    ui8_t bit;
    ui32_t val;
    if ((len < 0) || (len > 32)) return -1;
    val = 0;
    for (j = 0; j < len; j++) {
                bit = bits[j] - 0x30;
                val |= (bit << (len-1-j)); // big endian
                //val |= (bit << j);      // little endian
    }
    return val;
}
*/
ui32_t bits2val(ui8_t bits[], int len) {
    int j;
    ui8_t bit;
    ui32_t val;
    if ((len < 0) || (len > 32)) return -1;
    val = 0;
    for (j = 0; j < len; j++) {
                bit = bits[j];
                val |= (bit << (len-1-j)); // big endian
                //val |= (bit << j);      // little endian
    }
    return val;
}

/* -------------------------------------------------------------------------- */


int main(int argc, char **argv) {

    FILE *fp;
    char *fpname;
    int i, j;
    int bit_count = 0,
        header_found = 0,
        bit, len;

    int counter;
    ui32_t val;
    ui32_t dat2;
    int lat, lat1, lat2,
        lon, lon1, lon2,
        alt, alt1, alt2;
    ui16_t vH, vD;
     i16_t vU;
    double velH, velD, velU;
    int latdeg,londeg;
    double latmin, lonmin;
    ui32_t t1, t2, ms, min, std, tt, mm, jj;


#ifdef CYGWIN
    _setmode(fileno(stdin), _O_BINARY);  // _setmode(_fileno(stdin), _O_BINARY);
#endif
    setbuf(stdout, NULL);


    fpname = argv[0];
    ++argv;
    while ((*argv) && (!wavloaded)) {
        if      ( (strcmp(*argv, "-h") == 0) || (strcmp(*argv, "--help") == 0) ) {
        help_out:
            fprintf(stderr, "%s <-n> [options] audio.wav\n", fpname);
            fprintf(stderr, "  n=1,2\n");
            fprintf(stderr, "  options:\n");
            //fprintf(stderr, "       -v, --verbose\n");
            fprintf(stderr, "       -r, --raw\n");
            return 0;
        }
        else if ( (strcmp(*argv, "-r") == 0) || (strcmp(*argv, "--raw") == 0) ) {
            option_raw = 1;
        }
        else if   (strcmp(*argv, "--res") == 0) { option_res = 1; }
        else if ( (strcmp(*argv, "-i") == 0) || (strcmp(*argv, "--invert") == 0) ) {
            option_inv = 1;  // nicht noetig
        }
        else if ( (strcmp(*argv, "-2") == 0) ) {
            option2 = 1;
        }
        else if ( (strcmp(*argv, "-1") == 0) ) {
            option1 = 1;
        }
        else if   (strcmp(*argv, "-b") == 0) { option_b = 1; }
        else if   (strcmp(*argv, "--ecc") == 0) { option_ecc = 1; }
        else if ( (strcmp(*argv, "-v") == 0) ) {
            option_verbose = 1;
        }
        else if ( (strcmp(*argv, "--br") == 0) ) {
            ++argv;
            if (*argv) {
                baudrate = atof(*argv);
                if (baudrate < 2200 || baudrate > 2400) baudrate = 2400; // default: 2400
            }
            else return -1;
        }
        else {
            if ((option1 == 1  && option2 == 1) || (!option_raw && option1 == 0  && option2 == 0)) goto help_out;
            fp = fopen(*argv, "rb");
            if (fp == NULL) {
                fprintf(stderr, "%s konnte nicht geoeffnet werden\n", *argv);
                return -1;
            }
            wavloaded = 1;
        }
        ++argv;
    }
    if (!wavloaded) fp = stdin;


    i = read_wav_header(fp);
    if (i) {
        fclose(fp);
        return -1;
    }
    if (baudrate > 0) {
        samples_per_bit = sample_rate/baudrate; // default baudrate: 2400
        fprintf(stderr, "sps corr: %.4f\n", samples_per_bit);
    }

    if (option_ecc) {
        rs_init_BCH64();
    }


    bufpos = 0;
    bit_count = 0;

    while (!read_bits_fsk(fp, &bit, &len)) {

        if (len == 0) { // reset_frame();
/*
            if (byte_count > FRAME_LEN-20) {
                print_frame(byte_count);
                bit_count = 0;
                byte_count = FRAMESTART;
                header_found = 0;
            }
*/
            //inc_bufpos();
            //buf[bufpos] = 'x';
            continue;   // ...
        }

        for (i = 0; i < len; i++) {

            inc_bufpos();
            buf[bufpos] = 0x30 + bit;  // Ascii

            if (!header_found) {
                header_found = compare_subheader();
                if (header_found) {
                    bit_count = 0;
                    for (j = 0; j < HEADLEN; j++) {
                        if (header_found % 2 == 1) frame_bits[j] = header0x049DCEbits[j] - 0x30;
                        else                       frame_bits[j] = header0xFB6230bits[j] - 0x30;
                    }
                }
            }
            else {
                frame_rawbits[bit_count] = 0x30 + bit;
                bit_count++;

                if (option_b) {
                    while (++i < len) {
                        frame_rawbits[bit_count] = 0x30 + bit;
                        bit_count++;
                    }
                    bitstart = 1;
                    while (bit_count < RAWBITFRAME_LEN/4-RAWHEADLEN) {
                        if (read_rawbit(fp, &bit) == EOF) break;
                        frame_rawbits[bit_count] = 0x30 + bit;
                        bit_count++;
                    }
                }

                if (bit_count >= RAWBITFRAME_LEN/4-RAWHEADLEN) {  // 600-48
                    frame_rawbits[bit_count] = '\0';

                    biphi_s(frame_rawbits, frame_bits+HEADLEN);

                    if (option_ecc) {
                        for (block = 0; block < 6; block++) {

                            // prepare block-codeword
                            for (j =  0; j < 46; j++) cw[45-j] = frame_bits[HEADLEN + block*46+j];
                            for (j = 46; j < 63; j++) cw[j] = 0;

                            errors = rs_decode_bch_gf2t2(cw, err_pos, err_val);

                            // check parity,padding
                            if (errors >= 0) {
                                check_err = 0;
                                for (j = 46; j < 63; j++) { if (cw[j] != 0) check_err = 0x1; }
                                par = 1;
                                for (j = 13; j < 13+16; j++) par ^= cw[j];
                                if (cw[12] != par) check_err |= 0x100;
                                par = 1;
                                for (j = 30; j < 30+16; j++) par ^= cw[j];
                                if (cw[29] != par) check_err |= 0x10;
                                if (check_err) errors = -3;
                            }
                            if (errors >= 0) {
                                for (j = 0; j < 46; j++) frame_bits[HEADLEN + block*46+j] = cw[45-j];
                            }

                            if (errors < 0) block_err[block] = 0xE;
                            else            block_err[block] = errors;

                        }
                    }

                    if (!option2 && !option_raw) {

                        if (header_found % 2 == 1) {
                            val = bits2val(frame_bits+HEADLEN, 16);
                            counter = val & 0xFFFF;
                            if (counter % 2 == 0) printf("\n");
                            //printf("[0x%04X = %d] ", counter, counter);
                            printf("[%d] ", counter);

                            if (counter % 2 == 1) {
                                t2 = bits2val(frame_bits+HEADLEN+5*46  , 8);  // LSB
                                t1 = bits2val(frame_bits+HEADLEN+5*46+8, 8);
                                ms = (t1 << 8) | t2;
                                std = bits2val(frame_bits+HEADLEN+5*46+17, 8);
                                min = bits2val(frame_bits+HEADLEN+5*46+25, 8);
                                printf("  ");
                                printf("%02d:%02d:%06.3f ", std, min, (double)ms/1000.0);
                                printf("  ");

                                //printf("\n");
                            }
                        }

                        if (header_found % 2 == 0) {
                            val = bits2val(frame_bits+HEADLEN, 16);
                            //printf("%04x ", val & 0xFFFF);
                            if ((counter % 2 == 0))  { //  (val & 0xFFFF) > 0)  {// == 0x8080
                                //offset=24+16+1;

                                lat1 = bits2val(frame_bits+HEADLEN+46*0+17, 16);
                                lat2 = bits2val(frame_bits+HEADLEN+46*1   , 16);
                                lon1 = bits2val(frame_bits+HEADLEN+46*1+17, 16);
                                lon2 = bits2val(frame_bits+HEADLEN+46*2   , 16);
                                alt1 = bits2val(frame_bits+HEADLEN+46*2+17, 16);
                                alt2 = bits2val(frame_bits+HEADLEN+46*3   , 16);

                                lat = (lat1 << 16) | lat2;
                                lon = (lon1 << 16) | lon2;
                                alt = (alt1 << 16) | alt2;
                                //printf("%08X %08X %08X :  ", lat, lon, alt);
                                printf("  ");
                                printf("lat: %.5f  lon: %.5f  alt: %.2f", (double)lat/1e7, (double)lon/1e7, (double)alt/1e2);
                                printf("  ");

                                vH = bits2val(frame_bits+HEADLEN+46*3+17, 16);
                                vD = bits2val(frame_bits+HEADLEN+46*4   , 16);
                                vU = bits2val(frame_bits+HEADLEN+46*4+17, 16);
                                velH = (double)vH/1e2;
                                velD = (double)vD/1e2;
                                velU = (double)vU/1e2;
                                printf(" vH: %.2fm/s  D: %.1f  vV: %.2fm/s", velH, velD, velU);
                                printf("  ");

                                jj = bits2val(frame_bits+HEADLEN+5*46+ 8, 8) + 0x0700;
                                mm = bits2val(frame_bits+HEADLEN+5*46+17, 8);
                                tt = bits2val(frame_bits+HEADLEN+5*46+25, 8);
                                printf("  ");
                                printf("%4d-%02d-%02d ", jj, mm, tt);
                                printf("  ");

                                //printf("\n");
                            }
                        }

                    }
                    else if (option2 && !option_raw) {

                        if (header_found % 2 == 1) {
                            val = bits2val(frame_bits+HEADLEN, 16);
                            counter = val & 0xFFFF;
                            if (counter % 2 == 0) printf("\n");
                            //printf("[0x%04X = %d] ", counter, counter);
                            printf("[%d] ", counter);

                            if (counter % 2 == 0) {
                                t1 = bits2val(frame_bits+HEADLEN+5*46  , 8);  // MSB
                                t2 = bits2val(frame_bits+HEADLEN+5*46+8, 8);
                                ms = (t1 << 8) | t2;
                                std = bits2val(frame_bits+HEADLEN+5*46+17, 8);
                                min = bits2val(frame_bits+HEADLEN+5*46+25, 8);
                                printf("  ");
                                printf("%02d:%02d:%06.3f ", std, min, (double)ms/1000.0);
                                printf("  ");
                            }
                        }

                        if (header_found % 2 == 0) {
                            val = bits2val(frame_bits+HEADLEN, 16);
                            //printf("%04x ", val & 0xFFFF);
                            if ((counter % 2 == 0))  { //  (val & 0xFFFF) > 0)  {// == 0x2390
                                //offset=24+16+1;

                                dat2 = bits2val(frame_bits+HEADLEN, 16);
                                if (option_verbose) printf("%05u  ", dat2);
                                printf("(%02d-%02d-%02d) ", dat2/1000,(dat2/10)%100, (dat2%10)+10); // 2020: +20 ?

                                lat1 = bits2val(frame_bits+HEADLEN+46*0+17, 16);
                                lat2 = bits2val(frame_bits+HEADLEN+46*1   , 16);
                                lon1 = bits2val(frame_bits+HEADLEN+46*1+17, 16);
                                lon2 = bits2val(frame_bits+HEADLEN+46*2   , 16);
                                alt1 = bits2val(frame_bits+HEADLEN+46*2+17, 16);
                                alt2 = bits2val(frame_bits+HEADLEN+46*3   ,  8);

                                // NMEA?
                                lat = (lat1 << 16) | lat2;
                                lon = (lon1 << 16) | lon2;
                                alt = (alt1 <<  8) | alt2;
                                latdeg = (int)lat / 1e6;
                                latmin = (double)(lat/1e6-latdeg)*100/60.0;
                                londeg = (int)lon / 1e6;
                                lonmin = (double)(lon/1e6-londeg)*100/60.0;
                                //printf("%08X %08X %08X :  ", lat, lon, alt);
                                printf("  ");
                                printf("lat: %.5f  lon: %.5f  alt: %.2f", (double)latdeg+latmin, (double)londeg+lonmin, (double)alt/1e2);
                                printf("  ");

                                vD = bits2val(frame_bits+HEADLEN+46*4+17, 16);
                                vH = bits2val(frame_bits+HEADLEN+46*5   , 16);
                                velD = (double)vD/1e2;       // course, true
                                velH = (double)vH/1.94384e2; // speed: knots -> m/s
                                printf(" (vH: %.1fm/s  D: %.2f)", velH, velD);
                                printf("  ");
                            }
                            //else { printf("\n"); }
                        }

                    }
                    else { // raw

                        val = bits2val(frame_bits, HEADLEN);
                        printf("%06X ", val & 0xFFFFFF);
                        printf("  ");
                        for (i = 0; i < 6; i++) {

                            val = bits2val(frame_bits+HEADLEN+46*i   , 16);
                            printf("%04X ", val & 0xFFFF);

                            val = bits2val(frame_bits+HEADLEN+46*i+17, 16);
                            printf("%04X ", val & 0xFFFF);

                            val = bits2val(frame_bits+HEADLEN+46*i+34, 12);
                            //printf("%03X ", val & 0xFFF);
                            //printf(" ");
                        }
                        printf("\n");
                    }

                    bit_count = 0;
                    header_found = 0;

                    if (option_ecc && option_verbose) {
                        printf("#");
                        for (block = 0; block < 6; block++) printf("%X", block_err[block]);
                        printf("#  ");
                    }
                }
            }
        }
    }

    printf("\n");

    fclose(fp);

    return 0;
}

