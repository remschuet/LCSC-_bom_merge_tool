from typing import Dict, List, Any
import csv
import os
import logging
import sys


class BOMReader:
    ALLOWED_KEYS: List[str] = ["Description", "Qty", "Value", "LCSC"]

    def __init__(self, dir_path: str = ".") -> None:
        self.dir_path = dir_path

    def find_csv_files(self) -> List[str]:
        """Return a list of CSV file paths found in ``dir_path``.
        """
        files: List[str] = []
        for name in os.listdir(self.dir_path):
            if name.lower().endswith('.csv'):
                files.append(os.path.join(self.dir_path, name))
        return files

    def read_csv(self, file_path: str) -> List[Dict[str, str]]:
        """Read a CSV file and return a list of rows as dicts.
        """
        try:
            with open(file_path, mode='r', newline='', encoding='utf-8-sig') as fh:
                reader = csv.DictReader(fh)
                return [dict(row) for row in reader]
        except Exception:
            return []

    def filter_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """Return a copy of ``row`` containing only the allowed keys.
        """
        return {k: (row.get(k, '') if row is not None else '') for k in self.ALLOWED_KEYS}

    def read_all(self) -> Dict[str, List[Dict[str, str]]]:
        """Read all CSV files and return a mapping filename -> filtered rows.
        """
        result: Dict[str, List[Dict[str, str]]] = {}
        for path in self.find_csv_files():
            rows = self.read_csv(path)
            filtered = [self.filter_row(r) for r in rows]
            result[os.path.basename(path)] = filtered
        return result

    @staticmethod
    def _parse_qty(qty_raw: Any) -> float:
        """Try to parse a quantity value into a float.
        """
        if qty_raw is None:
            return 0.0
        if isinstance(qty_raw, (int, float)):
            return float(qty_raw)
        s = str(qty_raw).strip()
        if s == '':
            return 0.0
        # Remove thousands separators
        s = s.replace(',', '')
        try:
            return float(s)
        except Exception:
            return 0.0

    def merge_and_write(self, output_csv: str = 'merged.csv', log_path: str = 'info.log') -> None:
        """Merge all CSV files by the LCSC key, sum Qty and write results.
        """
        # configure logger with file + console handlers
        logger = logging.getLogger('bom_merge')
        logger.setLevel(logging.INFO)
        # Remove existing handlers if re-run
        logger.handlers.clear()
        fh = logging.FileHandler(log_path, mode='w', encoding='utf-8')
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)

        all_data = self.read_all()

        merged: Dict[str, Dict[str, Any]] = {}
        # Iterate per file so we can log contributors per-LCSC
        for filename, rows in all_data.items():
            for row in rows:
                lcsc = (row.get('LCSC') or '').strip()
                if lcsc == '':
                    key = ''
                else:
                    key = lcsc
                qty = self._parse_qty(row.get('Qty', ''))
                desc = (row.get('Description') or '').strip()
                val = (row.get('Value') or '').strip()

                entry = merged.setdefault(key, {
                    'Qty': 0.0,
                    'Descriptions': [],
                    'Values': [],
                    'Files': [],
                })
                entry['Qty'] += qty
                if desc and desc not in entry['Descriptions']:
                    entry['Descriptions'].append(desc)
                if val and val not in entry['Values']:
                    entry['Values'].append(val)
                # record file contribution for log (file:qty:desc)
                entry['Files'].append({'file': filename, 'qty': qty, 'desc': desc, 'value': val})

        # Write merged CSV
        try:
            with open(output_csv, mode='w', newline='', encoding='utf-8') as out_fh:
                writer = csv.DictWriter(out_fh, fieldnames=self.ALLOWED_KEYS)
                writer.writeheader()
                for lcsc_key, data in merged.items():
                    # Choose representative Description and Value (first if present)
                    rep_desc = data['Descriptions'][0] if data['Descriptions'] else ''
                    rep_val = data['Values'][0] if data['Values'] else ''
                    total_qty = data['Qty']
                    # Write Qty as int if whole number else float
                    if abs(total_qty - int(total_qty)) < 1e-9:
                        qty_out = str(int(total_qty))
                    else:
                        qty_out = str(total_qty)
                    writer.writerow({
                        'Description': rep_desc,
                        'Qty': qty_out,
                        'Value': rep_val,
                        'LCSC': lcsc_key,
                    })
        except Exception as e:
            logger.error(f"Failed to write merged CSV '{output_csv}': {e}")
            return
        # Log details only for collisions (multiple contributors for same LCSC)
        def _short(s: Any, max_len: int = 10) -> str:
            s2 = '' if s is None else str(s)
            if len(s2) <= max_len:
                return s2
            # keep max_len characters; indicate truncation with '..' if possible
            if max_len <= 2:
                return s2[:max_len]
            return s2[: max_len - 2] + '..'

        for lcsc_key, data in merged.items():
            # consider a collision when more than one contributing row exists
            if len(data.get('Files', [])) <= 1:
                continue
            total_qty = data['Qty']
            # Join and shorten values/descriptions
            descs = '; '.join(_short(d) for d in data['Descriptions']) if data['Descriptions'] else ''
            vals = '; '.join(_short(v) for v in data['Values']) if data['Values'] else ''
            contributors = ', '.join(
                f"{_short(c['file'])}(qty={_short(c['qty'])},desc='{_short(c['desc'])}',val='{_short(c['value'])}')"
                for c in data['Files']
            )
            logger.info(f"Merged LCSC='{_short(lcsc_key)}' total_qty={_short(total_qty)} Values=[{vals}] Descriptions=[{descs}] Contributors=[{contributors}]")

if __name__ == '__main__':
    reader = BOMReader(dir_path='./BOMs')
    reader.merge_and_write(output_csv='merged.csv', log_path='info.log')
