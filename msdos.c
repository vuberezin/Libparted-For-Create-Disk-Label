#include <parted/parted.h>
#include <parted/device.h>
#include <parted/debug.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* ripped from dos.c */

/* Read N sectors, starting with sector SECTOR_NUM (which has length
   DEV->sector_size) into malloc'd storage.  If the read fails, free
   the memory and return zero without modifying *BUF.  Otherwise, set
   *BUF to the new buffer and return 1.  */
int
ptt_read_sectors (PedDevice const *dev, PedSector start_sector,
		  PedSector n_sectors, void **buf)
{
  char *b = (char*)ped_malloc (n_sectors * dev->sector_size);
  PED_ASSERT (b != NULL);
  if (!ped_device_read (dev, b, start_sector, n_sectors)) {
    free (b);
    return 0;
  }
  *buf = b;
  return 1;
}

/* Read sector, SECTOR_NUM (which has length DEV->sector_size) into malloc'd
   storage.  If the read fails, free the memory and return zero without
   modifying *BUF.  Otherwise, set *BUF to the new buffer and return 1.  */
int
ptt_read_sector (PedDevice const *dev, PedSector sector_num, void **buf)
{
  return ptt_read_sectors (dev, sector_num, 1, buf);
}

/* note: lots of bit-bashing here, thus, you shouldn't look inside it.
 * Use chs_to_sector() and sector_to_chs() instead.
 */
typedef struct {
	uint8_t		head;
	uint8_t		sector;
	uint8_t		cylinder;
} __attribute__((packed)) RawCHS;


typedef struct _DosRawPartition	DosRawPartition;
/* ripped from Linux source */
struct _DosRawPartition {
    uint8_t		boot_ind;	/* 00:  0x80 - active */
	RawCHS		chs_start;	/* 01: */
	uint8_t		type;		/* 04: partition type */
	RawCHS		chs_end;	/* 05: */
	uint32_t	start;		/* 08: starting sector counting from 0 */
	uint32_t	length;		/* 0c: nr of sectors in partition */
} __attribute__((packed));


/* The maximum number of DOS primary partitions.  */
#define DOS_N_PRI_PARTITIONS	4

typedef struct _DosRawTable DosRawTable;
struct _DosRawTable {
	char			boot_code [440];
	uint32_t        mbr_signature;	/* really a unique ID */
	uint16_t        Unknown;
	DosRawPartition	partitions [DOS_N_PRI_PARTITIONS];
	uint16_t		magic;
} __attribute__((packed));

/* this MBR boot code is loaded into 0000:7c00 by the BIOS.  See mbr.s for
 * the source, and how to build it
 */

static const char MBR_BOOT_CODE[] = {
	0xfa, 0xb8, 0x00, 0x10, 0x8e, 0xd0, 0xbc, 0x00,
	0xb0, 0xb8, 0x00, 0x00, 0x8e, 0xd8, 0x8e, 0xc0,
	0xfb, 0xbe, 0x00, 0x7c, 0xbf, 0x00, 0x06, 0xb9,
	0x00, 0x02, 0xf3, 0xa4, 0xea, 0x21, 0x06, 0x00,
	0x00, 0xbe, 0xbe, 0x07, 0x38, 0x04, 0x75, 0x0b,
	0x83, 0xc6, 0x10, 0x81, 0xfe, 0xfe, 0x07, 0x75,
	0xf3, 0xeb, 0x16, 0xb4, 0x02, 0xb0, 0x01, 0xbb,
	0x00, 0x7c, 0xb2, 0x80, 0x8a, 0x74, 0x01, 0x8b,
	0x4c, 0x02, 0xcd, 0x13, 0xea, 0x00, 0x7c, 0x00,
	0x00, 0xeb, 0xfe
};


typedef unsigned char uuid_t[16];
/* gen_uuid.c */
extern void uuid_generate(uuid_t out);


/* hack: use the ext2 uuid library to generate a reasonably random (hopefully
 * with /dev/random) number.  Unfortunately, we can only use 4 bytes of it.
 * We make sure to avoid returning zero which may be interpreted as no FAT
 * serial number or no MBR signature.
 */
static inline uint32_t
generate_random_uint32 (void)
{
       union {
               uuid_t uuid;
               uint32_t i;
       } uu32;

       uuid_generate (uu32.uuid);

       return uu32.i > 0 ? uu32.i : 0xffffffff;
}

typedef struct _PC98RawPartition PC98RawPartition;

/* ripped from Linux/98 source */
struct _PC98RawPartition {
	uint8_t		mid;		/* 0x80 - boot */
	uint8_t		sid;		/* 0x80 - active */
	uint8_t		dum1;		/* dummy for padding */
	uint8_t		dum2;		/* dummy for padding */
	uint8_t		ipl_sect;	/* IPL sector */
	uint8_t		ipl_head;	/* IPL head */
	uint16_t	ipl_cyl;	/* IPL cylinder */
	uint8_t		sector;		/* starting sector */
	uint8_t		head;		/* starting head */
	uint16_t	cyl;		/* starting cylinder */
	uint8_t		end_sector;	/* end sector */
	uint8_t		end_head;	/* end head */
	uint16_t	end_cyl;	/* end cylinder */
	char		name[16];
} __attribute__((packed));


typedef struct {
	PedSector	ipl_sector;
	int		system;
	int		boot;
	int		hidden;
	char		name [17];
} PC98PartitionData;

#define MAX_CHS_CYLINDER 1021

static void
sector_to_chs (const PedDevice* dev, const PedCHSGeometry* bios_geom,
	       PedSector sector, RawCHS* chs)
{
	PedSector	real_c, real_h, real_s;

	PED_ASSERT (dev != NULL);
	PED_ASSERT (chs != NULL);

	if (!bios_geom)
		bios_geom = &dev->bios_geom;

	real_c = sector / (bios_geom->heads * bios_geom->sectors);
	real_h = (sector / bios_geom->sectors) % bios_geom->heads;
	real_s = sector % bios_geom->sectors;

	if (real_c > MAX_CHS_CYLINDER) {
		real_c = 1023;
		real_h = bios_geom->heads - 1;
		real_s = bios_geom->sectors - 1;
	}

	chs->cylinder = real_c % 0x100;
	chs->head = real_h;
	chs->sector = real_s + 1 + (real_c >> 8 << 6);
}

/* OrigState is information we want to preserve about the partition for
 * dealing with CHS issues
 */
typedef struct {
	PedGeometry	geom;
	DosRawPartition	raw_part;
	PedSector	lba_offset;	/* needed for computing start/end for
					 * logical partitions */
} OrigState;


typedef struct {
	unsigned char	system;
	int		boot;
	int		hidden;
	int		msftres;
	int		raid;
	int		lvm;
	int		lba;
	int		palo;
	int		prep;
	int		diag;
	int		irst;
	int		esp;
	int		bls_boot;
	OrigState*	orig;			/* used for CHS stuff */
} DosPartitionData;

static int
chs_get_cylinder (const RawCHS* chs)
{
	return chs->cylinder + ((chs->sector >> 6) << 8);
}

static int
chs_get_head (const RawCHS* chs)
{
	return chs->head;
}

/* counts from 0 */
static int
chs_get_sector (const RawCHS* chs)
{
	return (chs->sector & 0x3f) - 1;
}


/* This function attempts to infer the BIOS CHS geometry of the hard disk
 * from the CHS + LBA information contained in the partition table from
 * a single partition's entry.
 *
 * This involves some maths.  Let (c,h,s,a) be the starting cylinder,
 * starting head, starting sector and LBA start address of the partition.
 * Likewise, (C,H,S,A) the end addresses.  Using both of these pieces
 * of information, we want to deduce cyl_sectors and head_sectors which
 * are the sizes of a single cylinder and a single head, respectively.
 *
 * The relationships are:
 * c*cyl_sectors + h * head_sectors + s = a
 * C*cyl_sectors + H * head_sectors + S = A
 *
 * We can rewrite this in matrix form:
 *
 * [ c h ] [ cyl_sectors  ]  =  [ s - a ]  =  [ a_ ]
 * [ C H ] [ head_sectors ]     [ S - A ]     [ A_ ].
 *
 * (s - a is abbreviated to a_to simplify the notation.)
 *
 * This can be abbreviated into augmented matrix form:
 *
 * [ c h | a_ ]
 * [ C H | A_ ].
 *
 * Solving these equations requires following the row reduction algorithm.  We
 * need to be careful about a few things though:
 * 	- the equations might be linearly dependent, in which case there
 * 	are many solutions.
 * 	- the equations might be inconsistent, in which case there
 * 	are no solutions.  (Inconsistent partition table entry!)
 * 	- there might be zeros, so we need to be careful about applying
 * 	the algorithm.  We know, however, that C > 0.
 */

static int
probe_partition_for_geom (const PedPartition* part, PedCHSGeometry* bios_geom)
{
	DosPartitionData* dos_data;
	RawCHS* start_chs;
	RawCHS* end_chs;
	PedSector c, h, s, a, a_;	/* start */
	PedSector C, H, S, A, A_;	/* end */
	PedSector dont_overflow, denum;
	PedSector cyl_size, head_size;
	PedSector cylinders, heads, sectors;

	PED_ASSERT (part != NULL);
	PED_ASSERT (part->disk_specific != NULL);
	PED_ASSERT (bios_geom != NULL);

	dos_data = (DosPartitionData*)part->disk_specific;

	if (!dos_data->orig)
		return 0;

	start_chs = &dos_data->orig->raw_part.chs_start;
	c = chs_get_cylinder (start_chs);
	h = chs_get_head (start_chs);
	s = chs_get_sector (start_chs);
	a = dos_data->orig->geom.start;
	a_ = a - s;

	end_chs = &dos_data->orig->raw_part.chs_end;
	C = chs_get_cylinder (end_chs);
	H = chs_get_head (end_chs);
	S = chs_get_sector (end_chs);
	A = dos_data->orig->geom.end;
	A_ = A - S;

	if (h < 0 || H < 0 || h > 254 || H > 254)
		return 0;
	if (c > C)
		return 0;

	/* If no geometry is feasible, then don't even bother.
	 * Useful for eliminating assertions for broken partition
	 * tables generated by Norton Ghost et al.
	 */
	if (A > (C+1) * 255 * 63)
		return 0;

	/* Not enough information.  In theory, we can do better.  Should we? */
	if (C > MAX_CHS_CYLINDER)
		return 0;
	if (C == 0)
		return 0;

	/* Calculate the maximum number that can be multiplied by
	 * any head count without overflowing a PedSector
	 * 2^8 = 256, 8 bits + 1(sign bit) = 9
	 */
	dont_overflow = 1;
	dont_overflow <<= (8*sizeof(dont_overflow)) - 9;
	dont_overflow--;

	if (a_ > dont_overflow || A_ > dont_overflow)
		return 0;

	/* The matrix is solved by :
	 *
	 * [ c h | a_]			R1
	 * [ C H | A_]			R2
	 *
	 * (cH - Ch) cyl_size = a_H - A_h		H R1 - h R2
	 * => (if cH - Ch != 0) cyl_size = (a_H - A_h) / (cH - Ch)
	 *
	 * (Hc - hC) head_size = A_c - a_C		c R2 - C R1
	 * => (if cH - Ch != 0) head_size = (A_c - a_C) / (cH - Ch)
	 *
	 *   But this calculation of head_size would need
	 *   not overflowing A_c or a_C
	 *   So substitution is use instead, to minimize dimension
	 *   of temporary results :
	 *
	 * If h != 0 : head_size = ( a_ - c cyl_size ) / h
	 * If H != 0 : head_size = ( A_ - C cyl_size ) / H
	 *
	 */
	denum = c * H - C * h;
	if (denum == 0)
		return 0;

	cyl_size = (a_*H - A_*h) / denum;
	/* Check for non integer result */
	if (cyl_size * denum != a_*H - A_*h)
		return 0;

	if (!(cyl_size > 0))
		return 0;
	if (!(cyl_size <= 255 * 63))
		return 0;

	if (h > 0)
		head_size = ( a_ - c * cyl_size ) / h;
	else if (H > 0)
		head_size = ( A_ - C * cyl_size ) / H;
	else {
		/* should not happen because denum != 0 */
		PED_ASSERT (0);
	}

	if (!(head_size > 0))
		return 0;
	if (!(head_size <= 63))
		return 0;

	cylinders = part->disk->dev->length / cyl_size;
	heads = cyl_size / head_size;
	sectors = head_size;

	if (!(heads > 0))
		return 0;
	if (!(heads < 256))
		return 0;

	if (!(sectors > 0))
		return 0;
	if (!(sectors <= 63))
		return 0;

	/* Some broken OEM partitioning program(s) seem to have an out-by-one
	 * error on the end of partitions.  We should offer to fix the
	 * partition table...
	 */
	if (((C + 1) * heads + H) * sectors + S == A)
		C++;

	if (!((c * heads + h) * sectors + s == a))
		return 0;
	if (!((C * heads + H) * sectors + S == A))
		return 0;

	bios_geom->cylinders = cylinders;
	bios_geom->heads = heads;
	bios_geom->sectors = sectors;

	return 1;
}


static int
probe_filesystem_for_geom (const PedPartition* part, PedCHSGeometry* bios_geom)
{
	const char* ms_types[] = {"ntfs", "fat16", "fat32", NULL};
	int i;
	int found;
	unsigned char* buf;
	int sectors;
	int heads;
	int res = 0;

	PED_ASSERT (bios_geom        != NULL);
        PED_ASSERT (part             != NULL);
        PED_ASSERT (part->disk       != NULL);
        PED_ASSERT (part->disk->dev  != NULL);
        PED_ASSERT (part->disk->dev->sector_size % PED_SECTOR_SIZE_DEFAULT == 0);

        buf = (unsigned char*)ped_malloc (part->disk->dev->sector_size);

	if (!buf)
		return 0;

	if (!part->fs_type)
		goto end;

	found = 0;
	for (i = 0; ms_types[i]; i++) {
		if (!strcmp(ms_types[i], part->fs_type->name))
			found = 1;
	}
	if (!found)
		goto end;

	if (!ped_geometry_read(&part->geom, buf, 0, 1))
		goto end;

	/* shared by the start of all Microsoft file systems */
	sectors = buf[0x18] + (buf[0x19] << 8);
	heads = buf[0x1a] + (buf[0x1b] << 8);

	if (sectors < 1 || sectors > 63)
		goto end;
	if (heads > 255 || heads < 1)
		goto end;

	bios_geom->sectors = sectors;
	bios_geom->heads = heads;
	bios_geom->cylinders = part->disk->dev->length / (sectors * heads);
	res = 1;
end:
	free(buf);
	return res;
}


static void
partition_probe_bios_geometry (const PedPartition* part,
                               PedCHSGeometry* bios_geom)
{
	PED_ASSERT (part != NULL);
	PED_ASSERT (part->disk != NULL);
	PED_ASSERT (bios_geom != NULL);

	if (ped_partition_is_active (part)) {
		if (probe_partition_for_geom (part, bios_geom))
			return;
		if (part->type & PED_PARTITION_EXTENDED) {
			if (probe_filesystem_for_geom (part, bios_geom))
				return;
		}
	}
	if (part->type & PED_PARTITION_LOGICAL) {
		PedPartition* ext_part;
		ext_part = ped_disk_extended_partition (part->disk);
		PED_ASSERT (ext_part != NULL);
		partition_probe_bios_geometry (ext_part, bios_geom);
	} else {
		*bios_geom = part->disk->dev->bios_geom;
	}
}

#define PED_CPU_TO_LE16(x) (x)
#define BIT(x) (1 << (x))
#define SET_BIT(n,bit,val) n = (val)?  (n | BIT(bit))  :  (n & ~BIT(bit))
#define PED_CPU_TO_LE32(x) (x)

static int
fill_raw_part (DosRawPartition* raw_part,
               const PedPartition* part, PedSector offset)
{
	DosPartitionData*	dos_data;
	PedCHSGeometry		bios_geom;

	PED_ASSERT (raw_part != NULL);
	PED_ASSERT (part != NULL);

	partition_probe_bios_geometry (part, &bios_geom);

	dos_data = (DosPartitionData*)part->disk_specific;

	raw_part->boot_ind = 0x80 * dos_data->boot;
	raw_part->type = dos_data->system;
	raw_part->start = PED_CPU_TO_LE32 (part->geom.start - offset);
	raw_part->length = PED_CPU_TO_LE32 (part->geom.length);

	sector_to_chs (part->disk->dev, &bios_geom, part->geom.start,
		       &raw_part->chs_start);
	sector_to_chs (part->disk->dev, &bios_geom, part->geom.end,
		       &raw_part->chs_end);

	if (dos_data->orig) {
		DosRawPartition* orig_raw_part = &dos_data->orig->raw_part;
		if (dos_data->orig->geom.start == part->geom.start)
			raw_part->chs_start = orig_raw_part->chs_start;
		if (dos_data->orig->geom.end == part->geom.end)
			raw_part->chs_end = orig_raw_part->chs_end;
	}

	return 1;
}

#define PARTITION_DOS_EXT 0x05

static int
fill_ext_raw_part_geom (DosRawPartition* raw_part,
                        const PedCHSGeometry* bios_geom,
			const PedGeometry* geom, PedSector offset)
{
	PED_ASSERT (raw_part != NULL);
	PED_ASSERT (geom != NULL);
	PED_ASSERT (geom->dev != NULL);

	raw_part->boot_ind = 0;
	raw_part->type = PARTITION_DOS_EXT;
	raw_part->start = PED_CPU_TO_LE32 (geom->start - offset);
	raw_part->length = PED_CPU_TO_LE32 (geom->length);

	sector_to_chs (geom->dev, bios_geom, geom->start, &raw_part->chs_start);
	sector_to_chs (geom->dev, bios_geom, geom->start + geom->length - 1,
		       &raw_part->chs_end);

	return 1;
}


#define MSDOS_MAGIC 0xAA55

static int
write_ext_table (const PedDisk* disk,
                 PedSector sector, const PedPartition* logical)
{
	PedPartition*		part;
	PedSector		lba_offset;

	PED_ASSERT (disk != NULL);
	PED_ASSERT (ped_disk_extended_partition (disk) != NULL);
	PED_ASSERT (logical != NULL);

	lba_offset = ped_disk_extended_partition (disk)->geom.start;

	void* s;
	if (!ptt_read_sector (disk->dev, sector, &s))
		return 0;

	DosRawTable *table = (DosRawTable*)s;
	memset(&(table->partitions), 0, sizeof (table->partitions));
	table->magic = PED_CPU_TO_LE16 (MSDOS_MAGIC);

	int ok = 0;
	if (!fill_raw_part (&table->partitions[0], logical, sector))
		goto cleanup;

	part = ped_disk_get_partition (disk, logical->num + 1);
	if (part) {
		PedGeometry*		geom;
		PedCHSGeometry		bios_geom;

		geom = ped_geometry_new (disk->dev, part->prev->geom.start,
				part->geom.end - part->prev->geom.start + 1);
		if (!geom)
			goto cleanup;
		partition_probe_bios_geometry (part, &bios_geom);
		fill_ext_raw_part_geom (&table->partitions[1], &bios_geom,
				        geom, lba_offset);
		ped_geometry_destroy (geom);

		if (!write_ext_table (disk, part->prev->geom.start, part))
			goto cleanup;
	}

	ok = ped_device_write (disk->dev, table, sector, 1);
 cleanup:
	free (s);
	return ok;
}

static int
write_empty_table (const PedDisk* disk, PedSector sector)
{
	DosRawTable		table;
	void*			table_sector;

	PED_ASSERT (disk != NULL);

	if (ptt_read_sector (disk->dev, sector, &table_sector)) {
		memcpy (&table, table_sector, sizeof (table));
		free(table_sector);
	}
	memset (&(table.partitions), 0, sizeof (table.partitions));
	table.magic = PED_CPU_TO_LE16 (MSDOS_MAGIC);

	return ped_device_write (disk->dev, (void*) &table, sector, 1);
}


/* Find the first logical partition, and write the partition table for it.
 */
static int
write_extended_partitions (const PedDisk* disk)
{
	PedPartition*		ext_part;
	PedPartition*		part;
	PedCHSGeometry		bios_geom;

	PED_ASSERT (disk != NULL);

	ext_part = ped_disk_extended_partition (disk);
	partition_probe_bios_geometry (ext_part, &bios_geom);
	part = ped_disk_get_partition (disk, 5);
	if (part)
		return write_ext_table (disk, ext_part->geom.start, part);
	else
		return write_empty_table (disk, ext_part->geom.start);
}



static int
msdos_write (const PedDisk* disk)
{
	PedPartition* part;
	int			i;

	PED_ASSERT (disk != NULL);
	PED_ASSERT (disk->dev != NULL);

	void *s0;
	if (!ptt_read_sector (disk->dev, 0, &s0))
		return 0;

	DosRawTable *table = (DosRawTable*) s0;

	if (!table->boot_code[0]) {
		memset (table, 0, 512);
		memcpy (table->boot_code, MBR_BOOT_CODE, sizeof (MBR_BOOT_CODE));
	}

	/* If there is no unique identifier, generate a random one */
	if (!table->mbr_signature)
		table->mbr_signature = generate_random_uint32 ();

	memset (table->partitions, 0, sizeof (table->partitions));
	table->magic = PED_CPU_TO_LE16 (MSDOS_MAGIC);

	for (i=1; i<=DOS_N_PRI_PARTITIONS; i++) {
		part = ped_disk_get_partition (disk, i);
		if (!part)
			continue;

		if (!fill_raw_part (&table->partitions [i - 1], part, 0)){

				goto write_fail;
		}
		
		if (part->type == PED_PARTITION_EXTENDED) {
			if (!write_extended_partitions (disk))

				goto write_fail;
		}
	}
        int write_ok = ped_device_write(disk->dev, (void*) table, 0, 1);
        free (s0);
	if (!write_ok)
		return 0;
	return ped_device_sync (disk->dev);

 write_fail:
        free (s0);
        return 0;

}


int main (int argc, char* argv[])
{
    PedDevice *dev;
    PedDisk *disk;
    const PedDiskType* type;
    char dos[6] = "msdos";
    
    dev = ped_device_get ("/dev/sbulla");

    if (!ped_device_open (dev))
     	perror ("Error open device");
 
    type = ped_disk_type_get (dos);
    disk = ped_disk_new_fresh (dev, type);
    
    if (!disk)
     	perror("Error create disk");  

    msdos_write(disk);


    return 0;
}
